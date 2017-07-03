package draft;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb.TDBFactory;

public class NewClass3 {

    public static void main(String[] args) throws FileNotFoundException, IOException {

        String filename = "C:\\Users\\lapaesleme\\OneDrive\\Downloads\\teste2_2016-10-13_16-35-56.nq.gz";
        //Alternativa 1
        //Alimentando dataset em meoria com dados de um arquivo de dump compactado
        Model m = ModelFactory.createDefaultModel();
        Dataset ds1 = RDFDataMgr.loadDataset(filename);
        ds1.getNamedModel("http://linkedatacatalogg...");

        //Alternativa 2
        //Alimentando banco de dados de triplas local com dados de um arquivo de dump compactado
        new File("./resources/dat/tdb").mkdirs();
        String assemblerFile = "./resources/conf/voids.ttl";
        Dataset ds2 = TDBFactory.assembleDataset(assemblerFile);
        RDFDataMgr.read(ds2, filename);

        //Alternativa 3
        //Alimentando dataset de um servidor FUSEKI com dados de um arquivo de dump compactado
        String FUSEKI_DATA_URL = "http://localhost:8080/fuseki/voids2/data";
        DatasetAccessor ds3 = DatasetAccessorFactory.createHTTP(FUSEKI_DATA_URL);
        ds3.putModel(ds1.getDefaultModel());
        Iterator<String> iter = ds1.listNames();
        while (iter.hasNext()) {
            String name = iter.next();
            ds3.putModel(name, ds1.getNamedModel(name));
        }

        System.out.println("Salvo!");
    }
}
