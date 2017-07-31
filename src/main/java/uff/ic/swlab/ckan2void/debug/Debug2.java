package uff.ic.swlab.ckan2void.debug;


import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

public class Debug2 {

    public static void main(String[] args) {
        Dataset dataset = DatasetFactory.create();
        RDFDataMgr.read(dataset, "http://acm.rkbexplorer.com/models/void.ttl");

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

        queryString = String.format(queryString, "http://swlab.paes-leme.name:8080/resource/");
        Query query = QueryFactory.create(queryString);
        QueryExecution exec = QueryExecutionFactory.create(query, dataset);
        Model model = exec.execConstruct();

        queryString2 = String.format(queryString2, "http://swlab.paes-leme.name:8080/resource/", "http://swlab.paes-leme.name:8080/resource/rkb-explorer-acm-datahub");
        Query query2 = QueryFactory.create(queryString2);
        QueryExecution exec2 = QueryExecutionFactory.create(query2, model);
        Model model2 = exec2.execConstruct();

        RDFDataMgr.write(System.out, model2, Lang.TURTLE);

    }
}
