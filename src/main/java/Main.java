
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sdb.SDBFactory;
import org.apache.jena.sdb.Store;
import org.apache.jena.sdb.StoreDesc;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.core.CKANCrawler;
import uff.ic.swlab.ckan2void.core.Crawler;
import uff.ic.swlab.ckan2void.core.MakeVoIDTask;
import uff.ic.swlab.ckan2void.util.Config;

public abstract class Main {

    public static Config conf = Config.getInsatnce();

    public static void main(String[] args) {
        try {
            PropertyConfigurator.configure("./conf/log4j.properties");
            conf.host().initSDB(conf.datasetSDBDesc());
            conf.host().initSDB(conf.tempDatasetSDBDesc());

            run(args);

        } catch (Throwable e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public static void run(String[] args) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Config:");
        System.out.println("update host " + conf.host().hostname);
        System.out.println("task instances = " + conf.taskInstances());
        System.out.println("========================================");
        System.out.println("");

        String[] catalogs = conf.ckanCatalogs().split("[,\n\\p{Blank}]++");
        for (String catalog : catalogs) {

            if ((new UrlValidator()).isValid(catalog)) {

                Crawler<Dataset> crawler = new CKANCrawler(catalog);
                System.out.println("================================================================================================================================");
                System.out.println(String.format("Crawler started (%s).", catalog));
                int counter = 0;

                Dataset dataset;
                ExecutorService pool = Executors.newWorkStealingPool(conf.parallelism());
                while ((dataset = crawler.next()) != null) {

                    String graphURI = dataset.getJsonMetadataUrl();
                    try {
                        if (dataset.isUpdateCandidate()) {
                            pool.submit(new MakeVoIDTask(dataset, graphURI));
                            System.out.println((++counter) + ": Harvesting task of the dataset " + graphURI + " submitted.");
                        } else
                            System.out.println("Skipping dataset " + graphURI + ".");
                    } catch (Throwable t) {
                        System.out.println("Skipping dataset " + graphURI + ".");
                    }
                }

                pool.shutdown();
                System.out.println("Waiting for remaining tasks...");
                pool.awaitTermination(conf.poolShutdownTimeout(), conf.poolShutdownTimeoutUnit());

                System.out.println(String.format("Crawler ended (%s).", catalog));
                System.out.println("================================================================================================================================");
                System.out.println("");
            }

            System.gc();
        }

        createRootResource();
        exportDataset();
        uploadDataset();
    }

    private static void createRootResource() {
        System.out.println("Creating root resource on Fuseki...");
        String queryString = ""
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix foaf: <http://xmlns.com/foaf/0.1/>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "prefix : <http://swlab.paes-leme.name:8080/resource/>\n"
                + "\n"
                + "delete {?s ?p ?o.}\n"
                + "insert {:id-root-dataset-descriptions a void:DatasetDescription.\n"
                + "        :id-root-dataset-descriptions rdfs:label \"Root resource of the dataset Dataset Descriptions\".\n"
                + "        :id-root-dataset-descriptions foaf:topic ?s.}\n"
                + "where {\n"
                + "  select distinct ?s\n"
                + "  where {graph ?g {?s a void:Dataset.\n"
                + "                   filter not exists {?s2 (void:subset | void:classPartition | void:propertyPartition) ?s.}}}\n"
                + "}";

        try {
            queryString = String.format(queryString, conf.host().linkedDataNS());
            conf.host().execUpdate(queryString, conf.fusekiDataset());
            conf.host().execUpdate(queryString, StoreDesc.read(conf.datasetSDBDesc()));
        } catch (Throwable t) {
            Logger.getLogger("error").log(Level.ERROR, "Error while exporting dataset. Msg: " + t.getMessage());
            t.printStackTrace();
        }
        System.out.println("Done.");
    }

    private static void exportDataset() {
        System.out.println("Exporting dataset...");
        try {
            (new File(conf.localDatasetHomepageName())).getParentFile().mkdirs();
            Store datasetStore = SDBFactory.connectStore(Config.getInsatnce().datasetSDBDesc());
            try (OutputStream out = new FileOutputStream(conf.localNquadsDumpName());
                    GZIPOutputStream out2 = new GZIPOutputStream(out)) {
                RDFDataMgr.write(out2, SDBFactory.connectDataset(datasetStore), Lang.NQUADS);
                out2.finish();
                out.flush();
            }
        } catch (Throwable t) {
            Logger.getLogger("error").log(Level.ERROR, "Error while exporting dataset. Msg: " + t.getMessage());
            t.printStackTrace();
        }
        System.out.println("Done.");
    }

    private static void uploadDataset() {
        System.out.println("Uploading dataset...");
        try {
            Store datasetStore = SDBFactory.connectStore(Config.getInsatnce().datasetSDBDesc());
            org.apache.jena.query.Dataset dataset = SDBFactory.connectDataset(datasetStore);

            conf.host().mkDirsViaFTP(conf.remoteDatasetHomepageName(), conf.username(), conf.password());
            conf.host().uploadBinaryFile(conf.localDatasetHomepageName(), conf.remoteDatasetHomepageName(), conf.username(), conf.password());
            conf.host().uploadBinaryFile(conf.localNquadsDumpName(), conf.remoteNquadsDumpName(), conf.username(), conf.password());
            datasetStore.close();
        } catch (Throwable t) {
            Logger.getLogger("error").log(Level.ERROR, "Error while uploading dataset. Msg: " + t.getMessage());
            t.printStackTrace();
        }
        System.out.println("Done.");
    }

//    private static String getOper(String[] args) throws IllegalArgumentException {
//        String[] opers = {"insert", "upsert", "repsert"};
//        if (args == null || args.length == 0)
//            return "insert";
//        else if (args.length == 1 && args[0] != null && !args[0].equals(""))
//            if (Stream.of(opers).anyMatch(x -> x.equals(args[0])))
//                return args[0];
//        throw new IllegalArgumentException("Illegal argument list.");
//    }
}
