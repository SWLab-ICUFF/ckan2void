package test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.PropertyConfigurator;
import uff.ic.swlab.ckan2void.Config;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.core.CKANCrawler;
import uff.ic.swlab.ckan2void.core.MakeVoIDTask;

public class NewClass4 {

    public static void main(String[] args) throws InterruptedException, IOException {
        PropertyConfigurator.configure("./resources/conf/log4j.properties");
        Config.configure("./resources/conf/datasetcrawler.properties");
        CKANCrawler crawler = new CKANCrawler(Config.CKAN_CATALOG);

        Dataset dataset = crawler.getDataset("rkb-explorer-acm");
        String graphURI = dataset.getUri();

        ExecutorService pool = Executors.newWorkStealingPool(Config.PARALLELISM);
        pool.submit(new MakeVoIDTask(dataset, graphURI, Config.HOST));
        pool.shutdown();
        pool.awaitTermination(Config.POOL_SHUTDOWN_TIMEOUT, Config.POOL_SHUTDOWN_TIMEOUT_UNIT);
    }
}
