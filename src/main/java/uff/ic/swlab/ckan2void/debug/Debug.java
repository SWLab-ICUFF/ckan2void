package uff.ic.swlab.ckan2void.debug;


import eu.trentorise.opendata.jackan.CkanClient;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.naming.InvalidNameException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.util.VoIDHelper;
import uff.ic.swlab.util.Config;

public class Debug {

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException, InvalidNameException, SQLException {
        Config conf = Config.getInsatnce();

        CkanClient cc = new CkanClient("http://datahub.io");
        Dataset dataset = new Dataset(cc, "rkb-explorer-acm");

        Model _void = ModelFactory.createDefaultModel();
        Model _voidComp = ModelFactory.createDefaultModel();

        String graphUri = dataset.getJsonMetadataUrl();
        String datasetUri = dataset.getUri();
        String[] urls = dataset.getURLs();
        String[] sparqlEndPoints = dataset.getSparqlEndPoints();

        _void.add(dataset.toVoid());
        _voidComp.add(VoIDHelper.getContent(urls, sparqlEndPoints, conf.host().NS(), dataset.getUri()));
        System.out.println("*******************************");
        org.apache.jena.riot.RDFDataMgr.write(System.out, _voidComp, Lang.TURTLE);
        System.out.println("-------------------------------");
        conf.host().saveVoid(_void, _voidComp, datasetUri, graphUri);

    }
}
