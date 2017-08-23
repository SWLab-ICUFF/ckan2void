package uff.ic.swlab.util;

import java.io.File;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb.TDBFactory;

public class LoadTDB {

    public static void main(String[] args) {
        String dir = "C:/Program Files/Apache Software Foundation/Tomcat 8.5/webapps/fuseki/run/databases/DatasetDescriptions";
        (new File(dir)).mkdirs();
        Dataset dbpedia = TDBFactory.createDataset(dir);
        dbpedia.getDefaultModel().read("file://C:/Users/lapaesleme/Desktop/enrichments.dq");

        //List<String> d = null;
        //String[] d2 = d.toArray(new String[0]);
        //String lista = Arrays.toString(d2).replaceAll("\\[", "\\(").replaceAll("\\]", "\\)");
    }
}
