package draft;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

public class NewClass {

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void run(String[] args) {
        Model void_ = ModelFactory.createDefaultModel();
        void_.setNsPrefix("void", "http://rdfs.org/ns/void#");

        Resource voidDataset = void_.createResource("http://rdfs.org/ns/void#Dataset");
        Resource voidLinkset = void_.createResource("http://rdfs.org/ns/void#Linkset");
        Property voidSubset = void_.createProperty("http://rdfs.org/ns/void#subset");
        Property voidSubjectsTarget = void_.createProperty("http://rdfs.org/ns/void#sujectsTarget");
        Property voidObjectstarget = void_.createProperty("http://rdfs.org/ns/void#objectsTarget");

        Resource dataset = void_.createResource("a", voidDataset);
        String[] links = {"b", "c", "d"};
        int i = 0;
        for (String link : links) {
            i++;
            dataset.addProperty(voidSubset, void_.createResource("" + i, voidLinkset)
                    .addProperty(voidSubjectsTarget, dataset)
                    .addProperty(voidObjectstarget, void_.createResource(link)));
        }
        RDFDataMgr.write(System.out, void_, Lang.TTL);
    }
}
