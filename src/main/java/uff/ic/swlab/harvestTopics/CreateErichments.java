package uff.ic.swlab.harvestTopics;


import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.VOID;

public class CreateErichments {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        Dataset dataset = DatasetFactory.create();

        String NS = "http://swlab.ic.uff.br/resource/";
        String NS2 = "http://datahub.io/api/rest/dataset/";
        String NS3 = "http://linkeddatacatalog.dws.informatik.uni-mannheim.de/api/rest/dataset/";
        String[] datasetnames = {"rkb-explorer-acm", "rkb-explorer-ieee"};

        for (String datasetname : datasetnames) {
            Model model = dataset.getNamedModel(NS2 + datasetname);
            model.setNsPrefix("void", VOID.NS);
            model.setNsPrefix("dcterms", DCTerms.NS);
            model.setNsPrefix("", "http://swlab.ic.uff.br/resource/");

            Resource desc1 = model.createResource(NS + datasetname + "-datahub", VOID.Dataset)
                    .addProperty(VOID.subset, model.createResource(NS + "id-" + UUID.randomUUID().toString(), VOID.Dataset)
                            .addProperty(DCTerms.subject, model.createResource("http://dbpedia.org/category1"))
                            .addProperty(VOID.triples, model.createTypedLiteral(156l)))
                    .addProperty(VOID.subset, model.createResource(NS + "id-" + UUID.randomUUID().toString(), VOID.Dataset)
                            .addProperty(DCTerms.subject, model.createResource("http://dbpedia.org/category2"))
                            .addProperty(VOID.triples, model.createTypedLiteral(234l)));

            model = dataset.getNamedModel(NS3 + datasetname);
            model.setNsPrefix("void", VOID.NS);
            model.setNsPrefix("dcterms", DCTerms.NS);
            model.setNsPrefix("", "http://swlab.ic.uff.br/resource/");

            Resource desc2 = model.createResource(NS + datasetname + "-uni-mannheim", VOID.Dataset)
                    .addProperty(VOID.subset, model.createResource(NS + "id-" + UUID.randomUUID().toString(), VOID.Dataset)
                            .addProperty(DCTerms.subject, model.createResource("http://dbpedia.org/category1"))
                            .addProperty(VOID.triples, model.createTypedLiteral(156l)))
                    .addProperty(VOID.subset, model.createResource(NS + "id-" + UUID.randomUUID().toString(), VOID.Dataset)
                            .addProperty(DCTerms.subject, model.createResource("http://dbpedia.org/category2"))
                            .addProperty(VOID.triples, model.createTypedLiteral(234l)));

        }

        try (OutputStream out = new FileOutputStream("./data/v1/rdf/dataset/enrichments.nq.gz");
                GZIPOutputStream out2 = new GZIPOutputStream(out)) {
            RDFDataMgr.write(out2, dataset, RDFFormat.NQUADS);
            out2.finish();
            out.flush();
        }
    }
}
