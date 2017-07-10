package test;

import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;

public class NewClass1 {

    public static void main(String[] args) {
        DatasetAccessor accessor = DatasetAccessorFactory.createHTTP("dd");
        ((QueryEngineHTTP) accessor).setTimeout(1000l);
    }
}
