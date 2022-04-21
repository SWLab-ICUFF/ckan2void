package uff.ic.swlab.ckan2void.util;

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
import org.apache.jena.sparql.exec.http.QueryExecutionHTTP;

public abstract class VoIDHelper {

    public static Model getContent(String[] urls, String[] sparqlEndPoints, String namespace, String targetURI) throws
            InterruptedException {
        return getContentFromURL(urls, namespace, targetURI).add(getContentFromSparql(sparqlEndPoints, namespace, targetURI));
    }

    private static Model getContentFromURL(String[] urls, String namespace, String targetURI) throws
            InterruptedException {
        Model _void = ModelFactory.createDefaultModel();
        for (String url : makeVoIDUrls(urls))
            try {
            _void.add(extractVoID(RDFDataMgr.loadDataset(url, Config.getInsatnce().maxVoidFileSize()), namespace, targetURI));
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable e) {
        }
        return _void;
    }

    private static Model getContentFromSparql(String[] sparqlEndPoints, String namespace, String targetURI) throws
            InterruptedException {
        Model _void = ModelFactory.createDefaultModel();
        for (String endPoint : sparqlEndPoints)
            try {
            String[] graphs = VoIDHelper.detectVoidGraphNames(endPoint);
            if (graphs.length > 0) {
                String query = "construct {?s ?p ?o}\n %1$swhere {?s ?p ?o.}";
                String from = Arrays.stream(graphs).map((String n) -> String.format("from <%1$s>\n", n)).reduce("", String::concat);
                _void.add(extractVoID(RDFDataMgr.loadDataset(endPoint, String.format(query, from)), namespace, targetURI));
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable e) {
        }
        return _void;
    }

    public static Model extractPartitions(Model model, String targetURI) throws InterruptedException, ExecutionException,
            TimeoutException {
        String queryString = ""
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix owl: <http://www.w3.org/2002/07/owl#>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "construct {<%1$s> ?p1 ?s2. ?s2 ?p2 ?o2.}\n"
                + "where {\n"
                + "  {?s1 ?p1 ?s2.\n"
                + "   filter (?p1 in (void:subset, void:classPartition, void:propertyPartition)\n"
                + "           && not exists {?s2 a void:Linkset.})}\n"
                + "  ?s2 ?p2 ?o2.\n"
                + "}";
        Callable<Model> task = () -> {
            Query query = QueryFactory.create(String.format(queryString, targetURI));
            QueryExecution exec = QueryExecutionFactory.create(query, model);
            return exec.execConstruct();
        };
        return Executor.execute(task, "Extract partitions for " + targetURI, Config.getInsatnce().sparqlTimeout());
    }

    public static Model extractVoID(Dataset dataset, String namespace, String targetURI) throws InterruptedException,
            ExecutionException, TimeoutException {
        String queryString = ""
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix owl: <http://www.w3.org/2002/07/owl#>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "prefix null: <#>\n"
                + "prefix : <%1$s>\n"
                + "\n"
                + "construct {?u null:sameAs ?uu.\n"
                + "           ?s ?p ?o.}\n"
                + "where {\n"
                + "       {{{select distinct ?u"
                + "         where {{select distinct ?u where {?u ?p ?o.}}\n"
                + "                union {select distinct ?u where {?s ?p ?u. filter(isIRI(?u))}}}}\n"
                + "        bind(if(isBlank(?u),iri(\"%1$sid2-\"+struuid()),?u) as ?uu)}\n"
                + "       union {?s ?p ?o.}}\n"
                + "       union\n"
                + "       {graph ?g {{{select distinct ?u"
                + "                   where {{select distinct ?u where {?u ?p ?o.}}\n"
                + "                          union {select distinct ?u where {?s ?p ?u. filter(isIRI(?u))}}}}\n"
                + "                  bind(if(isBlank(?u),iri(\"%1$sid2-\"+struuid()),?u) as ?uu)}\n"
                + "                 union {?s ?p ?o.}}}"
                + "}";

        String queryString2 = ""
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix owl: <http://www.w3.org/2002/07/owl#>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "prefix null: <#>\n"
                + "prefix : <%1$s>\n"
                + "\n"
                + "construct {<%2$s> ?p1 ?s2.\n"
                + "           ?s2 ?p2 ?r3.\n"
                + "           ?s3 ?p3 ?r4.\n"
                + "           ?r4 ?p4 ?r5.\n"
                + "}\n"
                + "where {\n"
                + "  {?o1 ?p1 ?o2.\n"
                + "   filter (?p1 in (void:classPartition, void:propertyPartition)\n"
                + "           || (?p1 in (void:subset) && exists {?o2 a void:Linkset.}))}\n"
                + "  ?o2 ?p2 ?o3.\n"
                + "  ?o2 null:sameAs ?s2.\n"
                + "  optional {?o3 null:sameAs ?s3.}\n"
                + "  bind(if(bound(?s3),?s3,?o3) as ?r3)\n"
                + "  optional {?o3 (!<>)+ ?o4.\n"
                + "            optional {?o4 null:sameAs ?s4.}\n"
                + "            bind(if(bound(?s4),?s4,?o4) as ?r4)\n"
                + "            optional {?o3 ?p3 ?o4.}\n"
                + "            optional {?o4 ?p4 ?o5.}"
                + "            optional {?o5 null:sameAs ?s5.}\n"
                + "            bind(if(bound(?s5),?s5,?o5) as ?r5)"
                + "           }\n"
                + "}";

        String queryString3 = ""
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "prefix owl: <http://www.w3.org/2002/07/owl#>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "prefix : <%1$s>\n"
                + "\n"
                + "construct {?s ?p ?o.}\n"
                + "where {?s ?p ?o. filter(?p != <#sameAs>)}";

        Callable<Model> task = () -> {
            Query query = QueryFactory.create(String.format(queryString, namespace));
            QueryExecution exec = QueryExecutionFactory.create(query, dataset);

            query = QueryFactory.create(String.format(queryString2, namespace, targetURI));
            exec = QueryExecutionFactory.create(query, exec.execConstruct());

            query = QueryFactory.create(String.format(queryString3, namespace));
            exec = QueryExecutionFactory.create(query, exec.execConstruct());

            return exec.execConstruct();
        };
        return Executor.execute(task, "Extract void for " + targetURI, Config.getInsatnce().sparqlTimeout());
    }

    private static String[] makeVoIDUrls(String[] urls) {
        Set<String> voidURLs = new HashSet<>();

        try {
            for (String u : urls) {
                URL url = new URL(u);
                String protocol = url.getProtocol();
                String auth = url.getAuthority();
                String newPath = protocol + "://" + auth;
                voidURLs.add(newPath + "/.well-known/void");
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

    private static String[] detectVoidGraphNames(String sparqlEndPoint) throws InterruptedException, ExecutionException,
            TimeoutException {
        Callable<String[]> task = () -> {
            List<String> graphNames = new ArrayList<>();
            String queryString = "select distinct ?g where {graph ?g {?s ?p ?o.}}";
            String name;
            //try (final QueryExecution exec = new QueryEngineHTTP(sparqlEndPoint, queryString, HttpClients.createDefault())) {
            //    ((QueryEngineHTTP) exec).setTimeout(Config.getInsatnce().sparqlTimeout());
            try ( QueryExecution exec = QueryExecutionHTTP.service(sparqlEndPoint)
                    .query(queryString)
                    .timeout(Config.getInsatnce().sparqlTimeout())
                    .build()) {
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
        return Executor.execute(task, "Detect void graph names from " + sparqlEndPoint, Config.getInsatnce().sparqlTimeout());
    }

}
