package test;

import java.sql.SQLException;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sdb.SDBFactory;
import org.apache.jena.sdb.Store;
import org.apache.jena.sdb.util.StoreUtils;
import uff.ic.swlab.ckan2void.util.Config;

public class test {

    private static Config conf = Config.getInsatnce();

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Store store1 = SDBFactory.connectStore("./conf/sdb1.ttl");
        Store store2 = SDBFactory.connectStore("./conf/sdb2.ttl");

        Dataset dataset1 = SDBFactory.connectDataset(store1);
        Model model1 = dataset1.getDefaultModel();
        if (!StoreUtils.isFormatted(store1))
            store1.getTableFormatter().create();

        Dataset dataset2 = SDBFactory.connectDataset(store2);
        Model model2 = dataset2.getDefaultModel();
        if (!StoreUtils.isFormatted(store2))
            store2.getTableFormatter().create();

        store1.close();
        store2.close();
    }
}
