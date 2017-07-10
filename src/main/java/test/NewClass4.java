package test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.PropertyConfigurator;
import uff.ic.swlab.ckancrawler.core.CKANCrawler;
import uff.ic.swlab.ckancrawler.core.MakeVoIDTask;
import uff.ic.swlab.ckancrawler.adapter.Dataset;
import uff.ic.swlab.ckancrawler.adapter.FusekiServer;
import uff.ic.swlab.ckancrawler.Config;

public class NewClass4 {

    public static void main(String[] args) throws InterruptedException, IOException {
        PropertyConfigurator.configure("./resources/conf/log4j.properties");
        Config.configure("./resources/conf/datasetcrawler.properties");
        FusekiServer server = FusekiServer.getInstance(Config.FUSEKI_URL);
        CKANCrawler crawler = new CKANCrawler(Config.CKAN_CATALOG);

        Dataset dataset = crawler.getDataset("rkb-explorer-acm");
        String graphURI = dataset.getUri();

        ExecutorService pool = Executors.newWorkStealingPool(Config.PARALLELISM);
        pool.submit(new MakeVoIDTask(dataset, graphURI, server));
        pool.shutdown();
        pool.awaitTermination(Config.POOL_SHUTDOWN_TIMEOUT, Config.POOL_SHUTDOWN_TIMEOUT_UNIT);
    }
}
