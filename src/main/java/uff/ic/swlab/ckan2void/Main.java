package uff.ic.swlab.ckan2void;

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
import uff.ic.swlab.ckan2void.core.CKANCrawler;
import uff.ic.swlab.ckan2void.core.Crawler;
import uff.ic.swlab.ckan2void.core.MakeVoIDTask;
import uff.ic.swlab.ckan2void.util.Config;
import uff.ic.swlab.ckan2void.util.Dataset;

public abstract class Main {

    private static Config conf = Config.getInsatnce();

    public static void main(String[] args) {
        try {
            PropertyConfigurator.configure("./conf/log4j.properties");
            conf.host().initSDB(conf.datasetSDBDesc());
            conf.host().initSDB(conf.tempDatasetSDBDesc());

            while (true) {
                createDataset();
                System.gc();

                createRootResources();
                System.gc();

                exportDataset();
                System.gc();

                uploadDataset();
                System.gc();

                int hours = 8;
                System.out.println("Sleeping for " + hours + " hours.");
                Thread.sleep(1000 * 3600 * hours);
            }

        } catch (Throwable e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }

    public static void createDataset() throws IOException, InterruptedException {
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

                    String graphUri = dataset.getJsonMetadataUrl();
                    try {
                        if (dataset.isUpdateCandidate()) {
                            pool.submit(new MakeVoIDTask(dataset, graphUri));
                            System.out.println((++counter) + ": Harvesting task for " + graphUri + " submitted.");
                        } else
                            System.out.println("Skipping dataset " + graphUri + ".");
                    } catch (Throwable t) {
                        System.out.println("Skipping dataset " + graphUri + ".");
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
    }

    private static void createRootResources() {
        String queryString0 = "clear default";
        String queryString = ""
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix foaf: <http://xmlns.com/foaf/0.1/>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "prefix : <%1$s>\n"
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
            System.out.println("Creating root resource...");
            queryString = String.format(queryString, conf.host().NS());
            conf.host().execUpdate(queryString0, conf.fusekiDataset());
            conf.host().execUpdate(queryString, conf.fusekiDataset());
            conf.host().execUpdate(queryString0, StoreDesc.read(conf.datasetSDBDesc()));
            conf.host().execUpdate(queryString, StoreDesc.read(conf.datasetSDBDesc()));
            System.out.println("Done.");
        } catch (Throwable t) {
            Logger.getLogger("error").log(Level.ERROR, "Error while exporting dataset. Msg: " + t.getMessage());
            System.out.println("Failed.");
        }
    }

    private static void exportDataset() {
        try {
            System.out.println("Exporting dataset...");
            (new File(conf.localDatasetHomepageName())).getParentFile().mkdirs();
            Store datasetStore = SDBFactory.connectStore(Config.getInsatnce().datasetSDBDesc());
            try (OutputStream out = new FileOutputStream(conf.localNquadsDumpName());
                    GZIPOutputStream out2 = new GZIPOutputStream(out)) {
                RDFDataMgr.write(out2, SDBFactory.connectDataset(datasetStore), Lang.NQUADS);
                out2.finish();
                out.flush();
            } finally {
                datasetStore.getConnection().close();
            }
            System.out.println("Done.");
        } catch (Throwable t) {
            Logger.getLogger("error").log(Level.ERROR, "Error while exporting dataset. Msg: " + t.getMessage());
            System.out.println("Failed.");
        }
    }

    private static void uploadDataset() {
        try {
            System.out.println("Uploading dataset...");
            Store datasetStore = SDBFactory.connectStore(Config.getInsatnce().datasetSDBDesc());
            try {
                org.apache.jena.query.Dataset dataset = SDBFactory.connectDataset(datasetStore);
                conf.host().mkDirsViaFTP(conf.remoteDatasetHomepageName(), conf.username(), conf.password());
                conf.host().uploadBinaryFile(conf.localDatasetHomepageName(), conf.remoteDatasetHomepageName(), conf.username(), conf.password());
                conf.host().uploadBinaryFile(conf.localNquadsDumpName(), conf.remoteNquadsDumpName(), conf.username(), conf.password());
            } finally {
                datasetStore.getConnection().close();
            }
            System.out.println("Done.");
        } catch (Throwable t) {
            Logger.getLogger("error").log(Level.ERROR, "Error while uploading dataset. Msg: " + t.getMessage());
            System.out.println("Failed.");
        }
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
