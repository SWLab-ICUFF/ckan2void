package test;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import uff.ic.swlab.ckancrawler.adapter.FusekiServer;
import uff.ic.swlab.ckancrawler.core.MakeVoIDTask;
import uff.ic.swlab.ckancrawler.core.LODCrawler;

public class Main2 {

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void run(String[] args) throws MalformedURLException {
        Logger.getRootLogger().setLevel(Level.OFF);
        System.out.println("Crawler started.");

        FusekiServer server = FusekiServer.getInstance(null);
        server.dataUrl = "http://localhost:8080/fuseki/void/data";
        LODCrawler crawler = new LODCrawler();

        ExecutorService pool = Executors.newWorkStealingPool(20);
        while (crawler.hasNext()) {
            String[] urls = {crawler.next()};
            pool.submit(new MakeVoIDTask(null, null, server));
        }

        System.out.println("Done.");
    }

}
