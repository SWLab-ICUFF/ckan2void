package uff.ic.swlab.harvestTopics;

import uff.ic.swlab.util.ConnectionMySql;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
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


import java.sql.Connection;
import java.sql.SQLException;

public class Main {

    private static String NS = "http://swlab.ic.uff.br/resource/";
    private static String NS2 = "http://datahub.io/api/rest/dataset/";
    private static String NS3 = "http://linkeddatacatalog.dws.informatik.uni-mannheim.de/api/rest/dataset/";

    public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
        Dataset dataset = DatasetFactory.create();

        ArrayList<String> datasetnames = listDatasetnames();
        for (String datasetname : datasetnames) {

            ArrayList<Category> categories = listCategories(datasetname);
            for (Category category : categories) {
                Model model = dataset.getNamedModel(NS2 + datasetname);
                Resource desc1 = model.createResource(NS + datasetname + "-datahub", VOID.Dataset)
                        .addProperty(VOID.subset, model.createResource(NS + "id-" + UUID.randomUUID().toString(), VOID.Dataset)
                                .addProperty(DCTerms.subject, model.createResource(category.uri))
                                .addProperty(VOID.triples, model.createTypedLiteral(category.triples)));

                model = dataset.getNamedModel(NS3 + datasetname);
                Resource desc2 = model.createResource(NS + datasetname + "-uni-mannheim", VOID.Dataset)
                        .addProperty(VOID.subset, model.createResource(NS + "id-" + UUID.randomUUID().toString(), VOID.Dataset)
                                .addProperty(DCTerms.subject, model.createResource(category.uri))
                                .addProperty(VOID.triples, model.createTypedLiteral(category.triples)));
            }
        }

        try (OutputStream out = new FileOutputStream("./data/v1/rdf/dataset/enrichments.nq.gz");
                GZIPOutputStream out2 = new GZIPOutputStream(out)) {
            RDFDataMgr.write(out2, dataset, RDFFormat.NQUADS);
            out2.finish();
            out.flush();
        }
    }

    private static ArrayList<String> listDatasetnames() throws ClassNotFoundException, SQLException {

        ArrayList<String> datasetnames = new ArrayList<>();
        Connection conn = ConnectionMySql.Conectar();
        if (conn != null) {
            java.sql.Statement stmt = conn.createStatement();
            String query = "SELECT DISTINCT name_dataset FROM Types";
            java.sql.ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String name_dataset = rs.getString("name_dataset");
                datasetnames.add(name_dataset);
            }
            stmt.close();
            rs.close();
        }
        conn.close();

        //List<String> datasetnames = GetBD.listDatasetnames();
//
//        {// adaptar
//            datasetnames.add("rkb-explorer-acm");
//            datasetnames.add("rkb-explorer-ieee");
//        }
        return datasetnames;
    }

    private static class Category {

        public String uri;
        public long triples;

        public Category(String uri, long triples) {
            this.uri = uri;
            this.triples = triples;
        }
    }

    private static ArrayList<Category> listCategories(String datasetname) throws ClassNotFoundException, SQLException {
        ArrayList<Category> categories = new ArrayList<>();
        Connection conn = ConnectionMySql.Conectar();
        if (conn != null) {
            java.sql.Statement stmt = conn.createStatement();
            String query = "SELECT type_name, type_frequen FROM Types where name_dataset ='" + datasetname + "' ";
            java.sql.ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                categories.add(new Category(rs.getString("type_name"), rs.getLong("type_frequen")));
            }

            stmt.close();
            rs.close();
            conn.close();

        }

//        List<Category> categories = new ArrayList<>();
//
//        {//adaptar
//            categories.add(new Category("http://dbpedia.org/category1", 136));
//            categories.add(new Category("http://dbpedia.org/category2", 56));
//        }
        return categories;
    }
}
