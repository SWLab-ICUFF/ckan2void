package uff.ic.swlab.util;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb.TDBFactory;

public class Main {

    public static void main(String[] args) {
        String dir = "C:/Program Files/Apache Software Foundation/Tomcat 8.5/webapps/fuseki/run/databases/DBpedia201610";
        (new File(dir)).mkdirs();
        Dataset dbpedia = TDBFactory.createDataset(dir);
        dbpedia.getDefaultModel().read("file://C:/Users/lapaesleme/Desktop/skos_categories_en.ttl");

        List<String> d = null;
        String[] d2 = d.toArray(new String[0]);
        String lista = Arrays.toString(d2).replaceAll("\\[", "\\(").replaceAll("\\]", "\\)");
    }
}
