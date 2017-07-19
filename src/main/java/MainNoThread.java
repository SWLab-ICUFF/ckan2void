
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

        for (String catalog : Config.CKAN_CATALOGS.split("[,\n\\p{Blank}]++")) {
            Crawler<Dataset> c = new CKANCrawler(catalog);
            int counter = 0;
            while (c.hasNext()) {
                Dataset d = c.next();
                counter++;

                String[] urls = d.getURLs();
                String[] sparqlEndPoints = d.getSparqlEndPoints();

                String graphUri = d.getJsonMetadataUrl();

                Model model = d.toVoid();
                Model model2 = VoIDHelper.getContent(urls, sparqlEndPoints, d.getUri());

                System.out.println(counter + ": " + d.getJsonMetadataUrl());
                Config.HOST.saveVoid(model, model2, d.getUri(), graphUri);
            }
        }
    }
}
