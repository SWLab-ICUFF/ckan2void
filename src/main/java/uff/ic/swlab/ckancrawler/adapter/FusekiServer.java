package uff.ic.swlab.ckancrawler.adapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.naming.InvalidNameException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import uff.ic.swlab.ckancrawler.Config;

public class FusekiServer {

    public String quadsUrl = null;
    public String dataUrl = null;
    public String updateUrl = null;
    public String sparqlUrl = null;
    private static Map<String, FusekiServer> instances = new HashMap<>();

    private FusekiServer() {
    }

    private FusekiServer(String server) {
        quadsUrl = server;
        dataUrl = server + "/data";
        updateUrl = server + "/update";
        sparqlUrl = server + "/sparql";
    }

    public static FusekiServer getInstance(String server) {
        if (!instances.containsKey(server))
            instances.put(server, new FusekiServer(server));
        return instances.get(server);
    }

    public String getQuadsURL(String dataset) {
        return String.format(quadsUrl, dataset);
    }

    public String getDataURL(String dataset) {
        return String.format(dataUrl, dataset);
    }

    public String getSparqlURL(String dataset) {
        return String.format(sparqlUrl, dataset);
    }

    public String getUpdateURL(String dataset) {
        return String.format(updateUrl, dataset);
    }

    public synchronized List<String> listGraphNames(String dataset) {
        List<String> graphNames = new ArrayList<>();

        String queryString = "select distinct ?g where {graph ?g {[] ?p [].}}";
        try (QueryExecution exec = new QueryEngineHTTP(String.format(sparqlUrl, dataset), queryString)) {
            ((QueryEngineHTTP) exec).setTimeout(Config.SPARQL_TIMEOUT);
            ResultSet rs = exec.execSelect();
            while (rs.hasNext())
                graphNames.add(rs.next().getResource("g").getURI());
        } catch (Exception e) {
        }

        return graphNames;
    }

    public synchronized void loadDataset(String dataset, String uri) {
        Dataset ds = RDFDataMgr.loadDataset(uri);
        DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(getDataURL(dataset));
        accessor.putModel(ds.getDefaultModel());
        Iterator<String> iter = ds.listNames();
        while (iter.hasNext()) {
            String graphUri = iter.next();
            accessor.putModel(graphUri, ds.getNamedModel(graphUri));
        }
    }

    public synchronized void putModel(String dataset, String graphUri, Model model) throws InvalidNameException {
        if (graphUri != null && !graphUri.equals("")) {
            DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(getDataURL(dataset));
            accessor.putModel(graphUri, model);
            Logger.getLogger("info").log(Level.INFO, String.format("Dataset saved (<%1s>).", graphUri));
            return;
        }
        throw new InvalidNameException(String.format("Invalid graph URI: %1s.", graphUri));
    }

    public synchronized Model getModel(String dataset, String graphUri) throws InvalidNameException {
        if (graphUri != null && !graphUri.equals("")) {
            DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(getDataURL(dataset));
            Model model = accessor.getModel(graphUri);
            if (model != null)
                return model;
            else
                return ModelFactory.createDefaultModel();
        }
        throw new InvalidNameException(String.format("Invalid graph URI: %1s.", graphUri));
    }

}
