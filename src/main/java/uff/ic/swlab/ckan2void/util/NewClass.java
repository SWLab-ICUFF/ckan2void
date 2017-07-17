package uff.ic.swlab.ckan2void.util;

import eu.trentorise.opendata.jackan.CkanClient;
import javax.naming.InvalidNameException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import uff.ic.swlab.ckan2void.adapter.Dataset;

public class NewClass {

    public static void main(String[] args) throws InvalidNameException {
        CkanClient cc = new CkanClient("http://linkeddatacatalog.dws.informatik.uni-mannheim.de");
        Dataset d = new Dataset(cc, cc.getDataset("agrovoc-skos"));
        //Dataset d = new Dataset(cc, cc.getDataset("academic-offer-of-unl"));

        String graphUri = d.getJsonMetadataUrl();
        String derefGraphUri = Config.HOST.getQuadsURL(Config.FUSEKI_DATASET) + "?graph=" + graphUri;

        Model model = d.toVoid(derefGraphUri);

        org.apache.jena.riot.RDFDataMgr.write(System.out, model, Lang.TURTLE);
        SWLabHost.DEFAULT_HOST.putModel("DatasetDescriptions", d.getJsonMetadataUrl(), model);
    }
}
