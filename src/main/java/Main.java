
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
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

                Integer counter = 0;
                System.out.println(String.format("Crawler started (%s).", catalog));
                try (Crawler<Dataset> crawler = new CKANCrawler(catalog);) {

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
                System.gc();

            }

        org.apache.jena.query.Dataset dataset = DatasetFactory.create();
        RDFDataMgr.read(dataset, Config.HOST.getQuadsURL(Config.FUSEKI_DATASET));
        RDFDataMgr.write(new FileOutputStream(new File("")), dataset, Lang.NQ);
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
