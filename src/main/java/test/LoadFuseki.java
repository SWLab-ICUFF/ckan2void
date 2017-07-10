package test;

import java.io.File;
import java.util.Iterator;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb.TDBFactory;
import org.apache.log4j.PropertyConfigurator;

public class LoadFuseki {

    public static void main(String[] args) {
        PropertyConfigurator.configure("./resources/conf/log4j.properties");
        System.out.println("Loading local TDB...");
        String filename = "C:\\Users\\swlab\\OneDrive\\Uploads (lapaesleme)\\teste2_2016-10-13_16-35-56.nq.gz";
        new File("./resources/dat/tdb").mkdirs();
        String assemblerFile = "./resources/conf/teste2.ttl";
        Dataset ds1 = TDBFactory.assembleDataset(assemblerFile);
        RDFDataMgr.read(ds1, filename);

        System.out.println("Loading remote Fuseki...");
        String FUSEKI_DATA_URL = "http://swlab.ic.uff.br/fuseki/teste2/data";
        DatasetAccessor ds2 = DatasetAccessorFactory.createHTTP(FUSEKI_DATA_URL);
        ds2.putModel(ds1.getDefaultModel());
        Iterator<String> iter = ds1.listNames();
        int counter = 0;
        int total = countGraphs(ds1);
        while (iter.hasNext()) {
            counter++;
            String graphUri = iter.next();
            System.out.println("Loading graph " + graphUri + " (" + counter + "/" + total + ")...");
            ds2.putModel(graphUri, ds1.getNamedModel(graphUri));
        }
        System.out.println("Loaded!");
    }

    private static int countGraphs(Dataset ds) {
        int counter = 0;
        Iterator<String> iter = ds.listNames();
        while (iter.hasNext()) {
            counter++;
            iter.next();
        }
        return counter;
    }
}
