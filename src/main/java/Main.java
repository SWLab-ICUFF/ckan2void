
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import javax.naming.InvalidNameException;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.log4j.PropertyConfigurator;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.core.CKANCrawler;
import uff.ic.swlab.ckan2void.core.Crawler;
import uff.ic.swlab.ckan2void.core.MakeVoIDTask;
import uff.ic.swlab.ckan2void.util.Config;

public abstract class Main {

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Throwable e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public static void run(String[] args) throws IOException, InterruptedException, Exception {
        PropertyConfigurator.configure("./conf/log4j.properties");
        Config.configure("./conf/ckan2void.properties");
        Config.configureAuth("./conf/auth.properties");
        String oper = getOper(args);

        System.out.println("OPER = " + oper);
        for (String catalog : Config.CKAN_CATALOGS.split("[,\n\\p{Blank}]++"))
            if ((new UrlValidator()).isValid(catalog)) {

                try (Crawler<Dataset> crawler = new CKANCrawler(catalog);) {
                    System.out.println(String.format("Crawler started (%s).", catalog));
                    Integer counter = 0;

                    List<String> graphNames = Config.HOST.listGraphNames(Config.FUSEKI_DATASET, Config.SPARQL_TIMEOUT);
                    ExecutorService pool = Executors.newWorkStealingPool(Config.PARALLELISM);
                    while (crawler.hasNext()) {

                        Dataset dataset = crawler.next();
                        String graphURI = dataset.getJsonMetadataUrl();

                        if (oper == null || !oper.equals("insert") || (oper.equals("insert") && !graphNames.contains(graphURI))) {
                            pool.submit(new MakeVoIDTask(dataset, graphURI));
                            System.out.println((++counter) + ": Harvesting task of the dataset " + graphURI + " has been submitted.");
                        } else
                            System.out.println("Skipping dataset " + graphURI + ".");

                    }
                    pool.shutdown();
                    System.out.println("Waiting for remaining tasks...");
                    pool.awaitTermination(Config.POOL_SHUTDOWN_TIMEOUT, Config.POOL_SHUTDOWN_TIMEOUT_UNIT);

                }
                System.out.println(String.format("Crawler ended (%s).", catalog));
            }

        createRootResource();
        Config.HOST.backupDataset(Config.FUSEKI_DATASET);
    }

    private static void createRootResource() throws InvalidNameException {
        System.out.println("Creating root resource...");
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

        queryString = String.format(queryString, Config.HOST.linkedDataNS());
        Config.HOST.execUpdate(queryString, uff.ic.swlab.ckan2void.util.Config.FUSEKI_DATASET);
        System.out.println("Done.");
    }

    private static String getOper(String[] args) throws IllegalArgumentException {
        String[] opers = {"insert", "upsert", "repsert"};
        if (args == null || args.length == 0)
            return "insert";
        else if (args.length == 1 && args[0] != null && !args[0].equals(""))
            if (Stream.of(opers).anyMatch(x -> x.equals(args[0])))
                return args[0];
        throw new IllegalArgumentException("Illegal argument list.");
    }
}
