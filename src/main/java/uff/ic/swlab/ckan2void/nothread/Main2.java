package uff.ic.swlab.ckan2void.nothread;

import eu.trentorise.opendata.jackan.CkanClient;
import java.io.IOException;
import javax.naming.InvalidNameException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.log4j.PropertyConfigurator;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.helper.VoIDHelper;
import uff.ic.swlab.ckan2void.util.Config;

public class Main2 {

    public static void main(String[] args) throws InvalidNameException, IOException, InterruptedException {
        PropertyConfigurator.configure("./resources/conf/log4j.properties");
        Config.configure("./resources/conf/ckan2void.properties");

        CkanClient cc = new CkanClient("http://linkeddatacatalog.dws.informatik.uni-mannheim.de");

        Dataset d = new Dataset(cc, cc.getDataset("agrovoc-skos"));

        String[] urls = d.getURLs();
        String[] sparqlEndPoints = d.getSparqlEndPoints();

        String graphUri = d.getJsonMetadataUrl();
        String derefGraphUri = Config.HOST.getQuadsURL(Config.FUSEKI_DATASET) + "?graph=" + graphUri;

        Model model = d.toVoid(derefGraphUri);
        Model model2 = VoIDHelper.getContent(urls, sparqlEndPoints, d.getUri());

        org.apache.jena.riot.RDFDataMgr.write(System.out, model, Lang.TURTLE);
        System.out.println(d.getJsonMetadataUrl());
        Config.HOST.saveVoid(model, model2, d.getUri(), graphUri);
    }
}
