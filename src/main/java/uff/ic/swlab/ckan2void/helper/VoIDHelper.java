package uff.ic.swlab.ckan2void.helper;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import uff.ic.swlab.ckan2void.util.Config;
import uff.ic.swlab.ckan2void.util.Executor;
import uff.ic.swlab.ckan2void.util.RDFDataMgr;

public abstract class VoIDHelper {

    public static Model extractPartitions(Model model) throws InterruptedException, ExecutionException, TimeoutException {
        String queryString = ""
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix owl: <http://www.w3.org/2002/07/owl#>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "construct {?s1 ?p1 ?s2.\n"
                + "           ?s2 ?p2 ?o2.\n"
                //+ "           ?s3 ?p3 ?o3."
                + "}\n"
                + "where {\n"
                //+ "  ?s1 rdf:type void:Dataset.\n"
                //+ "  ?s1 (void:classPartition | void:propertyPartition) ?s2.\n"
                + "  {?s1 ?p1 ?s2.\n"
                + "  filter (?p1 in (void:subset, void:classPartition, void:propertyPartition)\n"
                + "          && not exists {?s2 a void:Linkset.})}\n"
                + "  optional {?s2 ?p2 ?o2.}\n"
                //+ "  ?s2 (!<>)* ?s3.\n"
                //+ "  optional {?s3 ?p3 ?o3. filter not exists {[] rdf:type ?s3}}\n"
                + "}";
        Callable<Model> task = () -> {
            Query query = QueryFactory.create(queryString);
            QueryExecution exec = QueryExecutionFactory.create(query, model);
            return exec.execConstruct();
        };
        return Executor.execute(task, Config.SPARQL_TIMEOUT);
    }

    public static Model extractVoID(Dataset dataset) throws InterruptedException, ExecutionException, TimeoutException {
        String queryString = ""
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix owl: <http://www.w3.org/2002/07/owl#>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "construct {?s1 rdf:type ?t1.\n"
                + "           ?s1 ?p1 ?o1.\n"
                + "           ?s2 ?p2 ?o2.}\n"
                + "where { "
                + "  {{?s1 ?p1 ?s2.\n"
                + "  filter (?p1 in (void:classPartition, void:propertyPartition))}\n"
                + "  optional {?s2 ?p2 ?o2.}}\n"
                + "\n"
                + "  union\n"
                + "  {{?s1 ?p1 ?s2.\n"
                + "   filter (?p1 = void:subset)}\n"
                + "   optional {?s2 ?p2 ?o2.}}\n"
                + "}";
        Callable<Model> task = () -> {
            Query query = QueryFactory.create(queryString);
            QueryExecution exec = QueryExecutionFactory.create(query, dataset);
            return exec.execConstruct();
        };
        return Executor.execute(task, Config.SPARQL_TIMEOUT);
    }

    public static Model extractVoIDOld(Dataset dataset) throws InterruptedException, ExecutionException, TimeoutException {
        String queryString = ""
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix owl: <http://www.w3.org/2002/07/owl#>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "construct {?s1 rdf:type ?t1.\n"
                + "           ?s1 ?p1 ?o1.\n"
                + "           ?s2 ?p2 ?o2.}\n"
                + "where { {\n"
                + "  {{?s1 rdf:type void:Dataset. bind(void:Dataset as ?t1)}\n"
                + "    union {?s1 rdf:type void:dataset. bind(void:Dataset as ?t1)}\n"
                + "    union {?s1 rdf:type void:DatasetDescription. bind(void:DatasetDescription as ?t1)}}\n"
                + "  ?s1 (!<>)* ?s2.\n"
                + "  optional {?s1 ?p1 ?o1.}\n"
                + "  optional {?s2 ?p2 ?o2. filter not exists {[] rdf:type ?s2}}\n"
                + "  }\n"
                + "  union {graph ?g {\n"
                + "    {{?s1 rdf:type void:Dataset. bind(void:Dataset as ?t1)}\n"
                + "      union {?s1 rdf:type void:dataset. bind(void:Dataset as ?t1)}\n"
                + "      union {?s1 rdf:type void:DatasetDescription. bind(void:DatasetDescription as ?t1)}}\n"
                + "    ?s1 (!<>)* ?s2.\n"
                + "    optional {?s1 ?p1 ?o1.}\n"
                + "    optional {?s2 ?p2 ?o2. filter not exists {[] rdf:type ?s2}}\n"
                + "    }\n"
                + "  }\n"
                + "}";
        Callable<Model> task = () -> {
            Query query = QueryFactory.create(queryString);
            QueryExecution exec = QueryExecutionFactory.create(query, dataset);
            return exec.execConstruct();
        };
        return Executor.execute(task, Config.SPARQL_TIMEOUT);
    }

    public static Model getContent(String[] urls, String[] sparqlEndPoints) throws InterruptedException {
        return getContentFromURL(urls).add(getContentFromSparql(sparqlEndPoints));
    }

    private static Model getContentFromURL(String[] urls) throws InterruptedException {
        Model _void = ModelFactory.createDefaultModel();
        for (String url : listVoIDUrls(urls))
            try {
                _void.add(extractVoID(RDFDataMgr.loadDataset(url, Config.MAX_VOID_FILE_SIZE)));
            } catch (InterruptedException e) {
                throw new InterruptedException();
            } catch (Throwable e) {
            }
        return _void;
    }

    private static Model getContentFromSparql(String[] sparqlEndPoints) throws InterruptedException {
        Model _void = ModelFactory.createDefaultModel();
        for (String endPoint : sparqlEndPoints)
            try {
                String[] graphs = VoIDHelper.listVoIDGraphNames(endPoint);
                if (graphs.length > 0) {
                    String query = "construct {?s ?p ?o}\n %1swhere {?s ?p ?o.}";
                    String from = Arrays.stream(graphs).map((String n) -> String.format("from <%1s>\n", n)).reduce("", String::concat);
                    _void.add(extractVoID(RDFDataMgr.loadDataset(String.format(query, from), endPoint)));
                }
            } catch (InterruptedException e) {
                throw new InterruptedException();
            } catch (Throwable e) {
            }
        return _void;
    }

    private static String[] listVoIDUrls(String[] urls) {
        Set<String> voidURLs = new HashSet<>();

        try {
            for (String u : urls) {
                URL url = new URL(u);
                String protocol = url.getProtocol();
                String auth = url.getAuthority();
                String newPath = protocol + "://" + auth;
                voidURLs.add(newPath);
                voidURLs.add(newPath + "/.well-known/void");
                voidURLs.add(newPath + "/.well-known/void.ttl");
                voidURLs.add(newPath + "/.well-known/void.rdf");
                voidURLs.add(newPath + "/void");
                voidURLs.add(newPath + "/void.ttl");
                voidURLs.add(newPath + "/void.rdf");
                voidURLs.add(newPath + "/models/void");
                voidURLs.add(newPath + "/models/void.ttl");
                voidURLs.add(newPath + "/models/void.rdf");
                String[] path = url.getPath().split("/");
                for (int i = 1; i < path.length; i++)
                    if (!path[i].contains("void")) {
                        newPath += "/" + path[i];
                        voidURLs.add(newPath);
                        voidURLs.add(newPath + "/void");
                        voidURLs.add(newPath + "/void.ttl");
                        voidURLs.add(newPath + "/void.rdf");
                    } else {
                        voidURLs.add(newPath + "/" + path[i]);
                        break;
                    }
            }
        } catch (Throwable e) {
        }

        return voidURLs.toArray(new String[0]);
    }

    private static String[] listVoIDGraphNames(String sparqlEndPoint) throws InterruptedException, ExecutionException, TimeoutException {
        Callable<String[]> task = () -> {
            List<String> graphNames = new ArrayList<>();
            String query = "select distinct ?g where {graph ?g {?s ?p ?o.}}";
            String name;
            try (final QueryExecution exec = new QueryEngineHTTP(sparqlEndPoint, query)) {
                ((QueryEngineHTTP) exec).setTimeout(Config.SPARQL_TIMEOUT);
                ResultSet rs = exec.execSelect();
                while (rs.hasNext()) {
                    name = rs.next().getResource("g").getURI();
                    if (name.contains("void"))
                        graphNames.add(name);
                }
            } catch (Throwable e) {
            }
            return graphNames.toArray(new String[0]);
        };
        return Executor.execute(task, Config.SPARQL_TIMEOUT);
    }
}
