

import java.io.IOException;
import javax.naming.InvalidNameException;
import org.apache.jena.rdf.model.Model;
import org.apache.log4j.PropertyConfigurator;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.core.CKANCrawler;
import uff.ic.swlab.ckan2void.core.Crawler;
import uff.ic.swlab.ckan2void.helper.VoIDHelper;
import uff.ic.swlab.ckan2void.util.Config;

public class MainNoThread {

    public static void main(String[] args) throws InvalidNameException, IOException, InterruptedException {
        PropertyConfigurator.configure("./resources/conf/log4j.properties");
        Config.configure("./resources/conf/ckan2void.properties");

        Crawler<Dataset> c = new CKANCrawler("http://linkeddatacatalog.dws.informatik.uni-mannheim.de");

        int counter = 0;
        while (c.hasNext()) {
            Dataset d = c.next();
            counter++;

            String[] urls = d.getURLs();
            String[] sparqlEndPoints = d.getSparqlEndPoints();

            String graphUri = d.getJsonMetadataUrl();
            String derefGraphUri = Config.HOST.getQuadsURL(Config.FUSEKI_DATASET) + "?graph=" + graphUri;

            Model model = d.toVoid(derefGraphUri);
            Model model2 = VoIDHelper.getContent(urls, sparqlEndPoints, d.getUri());

            System.out.println(counter + ": " + d.getJsonMetadataUrl());
            Config.HOST.saveVoid(model, model2, d.getUri(), graphUri);
        }
    }
}
