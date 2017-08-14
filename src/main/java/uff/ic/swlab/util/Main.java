package uff.ic.swlab.util;

import java.io.File;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb.TDBFactory;

public class Main {

    public static void main(String[] args) {
        String dir = "./data/tdb/DBpedia201610";
        (new File(dir)).mkdirs();
        Dataset dbpedia = TDBFactory.createDataset("./data/tdb/DBpedia201610");
        dbpedia.getDefaultModel().read("file://Volumes/Data/article_categories_en.ttl");
    }
}
