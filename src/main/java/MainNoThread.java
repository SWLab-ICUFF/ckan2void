
import javax.naming.InvalidNameException;
import org.apache.jena.rdf.model.Model;
import org.apache.log4j.PropertyConfigurator;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.core.CKANCrawler;
import uff.ic.swlab.ckan2void.core.Crawler;
import uff.ic.swlab.ckan2void.helper.VoIDHelper;
import uff.ic.swlab.ckan2void.util.Config;

public class MainNoThread {

    public static Config conf;

    public static void main(String[] args) throws InvalidNameException, InterruptedException {
        PropertyConfigurator.configure("./resources/conf/log4j.properties");
        conf = Config.getInsatnce();

        for (String catalog : conf.ckanCatalogs().split("[,\n\\p{Blank}]++")) {
            Crawler<Dataset> c = new CKANCrawler(catalog);
            int counter = 0;

            Dataset d;
            while ((d = c.next()) != null) {
                counter++;

                String[] urls = d.getURLs();
                String[] sparqlEndPoints = d.getSparqlEndPoints();

                String graphUri = d.getJsonMetadataUrl();

                Model model = d.toVoid();
                Model model2 = VoIDHelper.getContent(urls, sparqlEndPoints, d.getUri());

                System.out.println(counter + ": " + d.getJsonMetadataUrl());
                conf.host().saveVoid(model, model2, d.getUri(), graphUri, conf.fusekiDataset(), conf.fusekiTemDataset());
            }
        }
    }
}
