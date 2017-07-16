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

    public static Model extractPartitions(Model model, String targetURI) throws InterruptedException, ExecutionException, TimeoutException {
        String queryString = ""
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix owl: <http://www.w3.org/2002/07/owl#>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "construct {%1$s ?p1 ?s2.\n"
                + "           ?s2 ?p2 ?o2.\n"
                + "}\n"
                + "where {\n"
                + "  {?s1 ?p1 ?s2.\n"
                + "  filter (?p1 in (void:subset, void:classPartition, void:propertyPartition)\n"
                + "          && not exists {?s2 a void:Linkset.})}\n"
                + "  optional {?s2 ?p2 ?o2.}\n"
                + "}";
        Callable<Model> task = () -> {
            Query query = QueryFactory.create(String.format(queryString, targetURI));
            QueryExecution exec = QueryExecutionFactory.create(query, model);
            return exec.execConstruct();
        };
        return Executor.execute(task, "Extract partitions for " + targetURI, Config.SPARQL_TIMEOUT);
    }

    public static Model extractVoID(Dataset dataset, String targetURI) throws InterruptedException, ExecutionException, TimeoutException {
        String queryString = ""
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix owl: <http://www.w3.org/2002/07/owl#>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "construct {%1$s ?p1 ?s2.\n"
                + "           ?s2 ?p2 ?o2.}\n"
                + "where {\n"
                + "  {{?s1 ?p1 ?s2. filter (?p1 in (void:classPartition, void:propertyPartition)\n"
                + "                         || (?p1 in (void:subset) && exists {?s2 a void:Linkset.}))}\n"
                + "  optional {?s2 ?p2 ?o2.}}\n"
                + "  union\n"
                + "  {graph ?g {{?s1 ?p1 ?s2. filter (?p1 in (void:classPartition, void:propertyPartition)\n"
                + "                         || (?p1 in (void:subset) && exists {?s2 a void:Linkset.}))}\n"
                + "             optional {?s2 ?p2 ?o2.}}}\n"
                + "}";
        Callable<Model> task = () -> {
            Query query = QueryFactory.create(String.format(queryString, targetURI));
            QueryExecution exec = QueryExecutionFactory.create(query, dataset);
            return exec.execConstruct();
        };
        return Executor.execute(task, "Extract void for " + targetURI, Config.SPARQL_TIMEOUT);
    }

    public static Model getContent(String[] urls, String[] sparqlEndPoints, String targetURI) throws InterruptedException {
        return getContentFromURL(urls, targetURI).add(getContentFromSparql(sparqlEndPoints, targetURI));
    }

    private static Model getContentFromURL(String[] urls, String targetURI) throws InterruptedException {
        Model _void = ModelFactory.createDefaultModel();
        for (String url : makeVoIDUrls(urls))
            try {
                _void.add(extractVoID(RDFDataMgr.loadDataset(url, Config.MAX_VOID_FILE_SIZE), targetURI));
            } catch (InterruptedException e) {
                throw e;
            } catch (Throwable e) {
            }
        return _void;
    }

    private static Model getContentFromSparql(String[] sparqlEndPoints, String targetURI) throws InterruptedException {
        Model _void = ModelFactory.createDefaultModel();
        for (String endPoint : sparqlEndPoints)
            try {
                String[] graphs = VoIDHelper.detectVoidGraphNames(endPoint);
                if (graphs.length > 0) {
                    String query = "construct {?s ?p ?o}\n %1$swhere {?s ?p ?o.}";
                    String from = Arrays.stream(graphs).map((String n) -> String.format("from <%1$s>\n", n)).reduce("", String::concat);
                    _void.add(extractVoID(RDFDataMgr.loadDataset(String.format(query, from), endPoint), targetURI));
                }
            } catch (InterruptedException e) {
                throw e;
            } catch (Throwable e) {
            }
        return _void;
    }

    private static String[] makeVoIDUrls(String[] urls) {
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

    private static String[] detectVoidGraphNames(String sparqlEndPoint) throws InterruptedException, ExecutionException, TimeoutException {
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
        return Executor.execute(task, "List void graph names from " + sparqlEndPoint, Config.SPARQL_TIMEOUT);
    }

}
