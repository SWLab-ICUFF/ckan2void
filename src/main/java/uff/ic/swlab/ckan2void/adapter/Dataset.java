package uff.ic.swlab.ckan2void.adapter;

import eu.trentorise.opendata.jackan.CkanClient;
import eu.trentorise.opendata.jackan.model.CkanDataset;
import eu.trentorise.opendata.jackan.model.CkanDatasetRelationship;
import eu.trentorise.opendata.jackan.model.CkanPair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.WebContent;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.VOID;
import uff.ic.swlab.ckan2void.helper.URLHelper;
import uff.ic.swlab.ckan2void.util.Config;
import uff.ic.swlab.ckan2void.util.Executor;

public class Dataset {

    private final CkanDataset doc;
    private final CkanClient cc;

    public Dataset(CkanClient cc, CkanDataset doc) {
        this.doc = doc;
        this.cc = cc;
    }

    public String getCatalogUrl() {
        try {
            return cc.getCatalogUrl();
        } catch (Throwable e) {
            return "http://undefined-catalog";
        }
    }

    public String getName() {
        try {
            return doc.getName();
        } catch (Throwable e) {
            return "undefined-name";
        }
    }

    public String getNamespace() {
        try {
            return Config.HOST.linkedDataNS();
        } catch (Throwable e) {
            return "http://undefined-namespace/";
        }
    }

    public String getUri() {
        String sufix;
        if (getCatalogUrl().contains("uni-mannheim"))
            sufix = "-uni-mannheim";
        else if (getCatalogUrl().contains("datahub"))
            sufix = "-datahub";
        else
            sufix = "";
        return getNamespace() + getName() + sufix;
    }

    public String getJsonMetadataUrl() {
        return getCatalogUrl() + "/api/rest/dataset/" + getName();
    }

    public String getJsonFullMetadataUrl() {
        return getCatalogUrl() + "/api/3/action/package_show?id=" + getName();
    }

    public String getWebMetadataUrl() {
        return getCatalogUrl() + "/dataset/" + getName();
    }

    public String getUrl() {
        try {
            String url = doc.getUrl();
            return URLHelper.normalize(url);
        } catch (Throwable e) {
            return null;
        }
    }

    public String getTitle() {
        try {
            String title = doc.getTitle();
            if (!title.equals(""))
                return title;
        } catch (Throwable t) {
        }
        return null;
    }

    public String getNotes() {
        try {
            String notes = doc.getNotes();
            if (!notes.equals(""))
                return notes;
        } catch (Throwable t) {
        }
        return null;
    }

    public Calendar getMetadataCreated() {
        try {
            Calendar cal = Calendar.getInstance();
            cal.setTime(doc.getMetadataCreated());
            return cal;
        } catch (Throwable t) {
            return null;
        }
    }

    public Calendar getMetadataModified() {
        try {
            Calendar cal = Calendar.getInstance();
            cal.setTime(doc.getMetadataModified());
            return cal;
        } catch (Throwable t) {
            return null;
        }
    }

    public String[] getTags() {
        try {
            List<String> tags = new ArrayList<>();
            doc.getTags().stream().forEach((tag) -> {
                try {
                    if (!tag.getName().equals(""))
                        tags.add(tag.getName().trim());
                } catch (Throwable t) {
                }
            });
            return (new HashSet<>(tags)).toArray(new String[0]);
        } catch (Throwable e) {
            return new String[0];
        }
    }

    public String[] getNamespaces() {
        try {
            List<String> uriSpaces = new ArrayList<>();
            doc.getExtras().stream().forEach((extra) -> {
                try {
                    if (extra.getKey().toLowerCase().contains("namespace"))
                        uriSpaces.add(extra.getValue().trim());
                } catch (Throwable e) {
                }
            });
            return (new HashSet<>(uriSpaces)).toArray(new String[0]);
        } catch (Throwable e) {
            return new String[0];
        }
    }

    public String[] getVoIDUrls() {
        try {
            List<String> voids = new ArrayList<>();
            doc.getResources().stream().forEach((resource) -> {
                try {
                    if (resource.getDescription().toLowerCase().contains("void")
                            || resource.getFormat().toLowerCase().contains("void")
                            || resource.getUrl().toLowerCase().contains("void"))
                        voids.add(URLHelper.normalize(resource.getUrl()));
                } catch (Throwable e) {
                }
            });
            return (new HashSet<>(voids)).toArray(new String[0]);
        } catch (Throwable e) {
            return new String[0];
        }
    }

    public String[] getExampleUrls() {
        try {
            List<String> examples = new ArrayList<>();
            doc.getResources().stream().forEach((resource) -> {
                try {
                    if (resource.getDescription().toLowerCase().contains("example")
                            || resource.getFormat().toLowerCase().contains("example")
                            || resource.getUrl().toLowerCase().contains("example"))
                        examples.add(URLHelper.normalize(resource.getUrl()));
                } catch (Throwable e) {
                }
            });
            return (new HashSet<>(examples)).toArray(new String[0]);
        } catch (Throwable e) {
            return new String[0];
        }
    }

    public String[] getDumpUrls() {
        try {
            List<String> dumps = new ArrayList<>();
            doc.getResources().stream().forEach((resource) -> {
                try {
                    if ((resource.getDescription().toLowerCase().contains("dump")
                            || resource.getFormat().toLowerCase().contains("dump")
                            || resource.getUrl().toLowerCase().contains("dump"))
                            && !resource.getDescription().toLowerCase().contains("example")
                            && !resource.getFormat().toLowerCase().contains("example")
                            && !resource.getUrl().toLowerCase().contains("example"))
                        dumps.add(URLHelper.normalize(resource.getUrl()));
                } catch (Throwable e) {
                }
            });
            return (new HashSet<>(dumps)).toArray(new String[0]);
        } catch (Throwable e) {
            return new String[0];
        }
    }

    public String[] getSparqlEndPoints() {
        try {
            List<String> sparqlEndPoints = new ArrayList<>();
            doc.getResources().stream().forEach((resource) -> {
                try {
                    if (resource.getDescription().toLowerCase().contains("sparql")
                            || resource.getFormat().toLowerCase().contains("sparql")
                            || resource.getUrl().toLowerCase().contains("sparql"))
                        sparqlEndPoints.add(URLHelper.normalize(resource.getUrl()));
                } catch (Throwable e) {
                }
            });
            return (new HashSet<>(sparqlEndPoints)).toArray(new String[0]);
        } catch (Throwable e) {
            return new String[0];
        }
    }

    public String[] getURLs() {
        String[] urls;
        Set<String> set = new HashSet<>();
        set.add(getUrl());
        set.addAll(Arrays.asList(getNamespaces()));
        set.addAll(Arrays.asList(getVoIDUrls()));
        set.addAll(Arrays.asList(getExampleUrls()));
        set.addAll(Arrays.asList(getDumpUrls()));
        set = set.stream().filter((String line) -> line != null).collect(Collectors.toSet());
        urls = set.toArray(new String[0]);
        return urls;
    }

    private Set<Entry<String, Integer>> getLinks() {
        Map<String, Integer> links = new HashMap<>();
        String ns = getNamespace();
        try {
            String key, value;
            for (CkanPair ex : doc.getExtras())
                try {
                    key = ex.getKey();
                    value = ex.getValue();
                    if (key.startsWith("links:"))
                        links.put(ns + key.replace("links:", "").trim().replaceAll(" ", "_"), Integer.parseInt(value));
                } catch (Throwable e) {
                }
        } catch (Throwable e) {
        }
        return links.entrySet();
    }

    private Set<Entry<String, Integer>> getLinks2() {
        Map<String, Integer> links = new HashMap<>();
        String ns = getNamespace();
        try {
            for (CkanDatasetRelationship rel : doc.getRelationshipsAsSubject())
                links.put(ns + cc.getDataset(rel.getObject()).getName(), Integer.parseInt(rel.getComment()));
        } catch (Throwable e) {
        }
        return links.entrySet();
    }

    private Integer getTriples() {
        try {
            List<CkanPair> extras = doc.getExtras();
            for (CkanPair d : extras)
                if (d.getKey().trim().toLowerCase().equals("triples"))
                    return Integer.parseInt(d.getValue());
        } catch (Throwable e) {
        }
        return null;
    }

    public Set<Entry<String, Integer>> getClasses() {
        for (String sparqlEndPoint : getSparqlEndPoints())
            try {
                String queryString = "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                        + "select ?class (count(?s) as ?freq)\n"
                        + "WHERE {{?s rdf:type ?class} union {graph ?g {?s rdf:type ?class}}}\n"
                        + "group by ?class\n"
                        + "order by desc(?freq)\n"
                        + "limit 200";
                Callable<Map<String, Integer>> task = () -> {
                    Map<String, Integer> classes = new HashMap<>();
                    try (QueryExecution exec = new QueryEngineHTTP(sparqlEndPoint, queryString, HttpClients.createDefault())) {
                        ((QueryEngineHTTP) exec).setModelContentType(WebContent.contentTypeRDFXML);
                        ((QueryEngineHTTP) exec).setTimeout(Config.SPARQL_TIMEOUT);
                        ResultSet rs = exec.execSelect();
                        while (rs.hasNext()) {
                            QuerySolution qs = rs.next();
                            Resource class_ = qs.getResource("class");
                            Literal freq = qs.getLiteral("freq");
                            try {
                                if (!class_.getURI().equals(""))
                                    classes.put(class_.getURI(), freq.getInt());
                            } catch (Throwable t) {
                            }
                        }
                        return classes;
                    }
                };
                return Executor.execute(task, "Query classes from " + sparqlEndPoint, Config.SPARQL_TIMEOUT).entrySet();
            } catch (InterruptedException e) {
            } catch (Throwable e) {
            }
        return (new HashMap<String, Integer>()).entrySet();
    }

    public Set<Entry<String, Integer>> getProperties() {
        for (String sparqlEndPoint : getSparqlEndPoints())
            try {
                String queryString = "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                        + "select ?property (count(?s) as ?freq)\n"
                        + "WHERE {{?s ?property []} union {graph ?g {?s ?property []}}}\n"
                        + "group by ?property\n"
                        + "order by desc(?freq)\n"
                        + "limit 200";
                Callable<Map<String, Integer>> task = () -> {
                    Map<String, Integer> properties = new HashMap<>();
                    try (QueryExecution exec = new QueryEngineHTTP(sparqlEndPoint, queryString, HttpClients.createDefault())) {
                        ((QueryEngineHTTP) exec).setModelContentType(WebContent.contentTypeRDFXML);
                        ((QueryEngineHTTP) exec).setTimeout(Config.SPARQL_TIMEOUT);
                        ResultSet rs = exec.execSelect();
                        while (rs.hasNext()) {
                            QuerySolution qs = rs.next();
                            Resource property = qs.getResource("property");
                            Literal freq = qs.getLiteral("freq");
                            try {
                                if (!property.getURI().equals(""))
                                    properties.put(property.getURI(), freq.getInt());
                            } catch (Throwable t) {
                            }
                        }
                        return properties;
                    }
                };
                return Executor.execute(task, "Query properties from " + sparqlEndPoint, Config.SPARQL_TIMEOUT).entrySet();
            } catch (InterruptedException e) {
            } catch (Throwable e) {
            }
        return (new HashMap<String, Integer>()).entrySet();
    }

    public Model toVoid(String derefGraphUri) {
        String ns = Config.HOST.linkedDataNS();

        Model _void = ModelFactory.createDefaultModel();
        _void.setNsPrefix("rdf", RDF.uri);
        _void.setNsPrefix("rdfs", RDFS.uri);
        _void.setNsPrefix("owl", OWL.NS);
        _void.setNsPrefix("dcterms", DCTerms.NS);
        _void.setNsPrefix("foaf", FOAF.NS);
        _void.setNsPrefix("void", VOID.NS);
        _void.setNsPrefix("", ns);

        Resource dataset = _void.createResource(getUri(), VOID.Dataset);

        Set<Entry<String, Integer>> links = getLinks();
        links.addAll(getLinks2());
        links.stream().forEach((link) -> {
            dataset.addProperty(VOID.subset, _void.createResource(ns + "id-" + UUID.randomUUID().toString(), VOID.Linkset)
                    .addProperty(VOID.subjectsTarget, dataset)
                    .addProperty(VOID.objectsTarget, _void.createResource(link.getKey()))
                    .addLiteral(VOID.triples, _void.createTypedLiteral(link.getValue())));
        });

        Set<Entry<String, Integer>> classes = getClasses();
        classes.stream().forEach((_class) -> {
            dataset.addProperty(VOID.classPartition, _void.createResource(ns + "id-" + UUID.randomUUID().toString(), VOID.Dataset)
                    .addProperty(VOID._class, _class.getKey())
                    .addLiteral(VOID.entities, _void.createTypedLiteral(_class.getValue())));
        });

        Set<Entry<String, Integer>> properties = getProperties();
        properties.stream().forEach((property) -> {
            dataset.addProperty(VOID.propertyPartition, _void.createResource(ns + "id-" + UUID.randomUUID().toString(), VOID.Dataset)
                    .addProperty(VOID.property, property.getKey())
                    .addLiteral(VOID.triples, _void.createTypedLiteral(property.getValue())));
        });

        List<String> sparqlEndpoints = Arrays.asList(getSparqlEndPoints());
        sparqlEndpoints.stream().forEach((sparqlEndpoint) -> {
            dataset.addProperty(VOID.sparqlEndpoint, _void.createResource(sparqlEndpoint));
        });

        List<String> dumps = Arrays.asList(getDumpUrls());
        dumps.stream().forEach((dumpURL) -> {
            dataset.addProperty(VOID.dataDump, _void.createResource(dumpURL));
        });

        List<String> uriSpaces = Arrays.asList(getNamespaces());
        uriSpaces.stream().forEach((uriSpace) -> {
            dataset.addProperty(VOID.uriSpace, uriSpace);
        });

        List<String> tags = Arrays.asList(getTags());
        tags.stream().forEach((tag) -> {
            dataset.addProperty(FOAF.topic, tag);
        });

        String title = getTitle();
        if (title != null && !title.equals("")) {
            dataset.addProperty(DCTerms.title, title);
            dataset.addProperty(RDFS.label, title);
        }

        String description = getNotes();
        if (description != null && !description.equals(""))
            dataset.addProperty(DCTerms.description, description);

        String homepage = getUrl();
        if (homepage != null && !homepage.equals(""))
            dataset.addProperty(FOAF.homepage, _void.createResource(homepage));

        String page1 = getJsonMetadataUrl();
        if (page1 != null && !page1.equals(""))
            dataset.addProperty(RDFS.seeAlso, _void.createResource(page1));

        String page2 = getJsonFullMetadataUrl();
        if (page2 != null && !page2.equals(""))
            dataset.addProperty(RDFS.seeAlso, _void.createResource(page2));

        String page3 = getWebMetadataUrl();
        if (page3 != null && !page3.equals(""))
            dataset.addProperty(RDFS.seeAlso, _void.createResource(page3));

        Integer triples = getTriples();
        if (triples != null)
            dataset.addProperty(VOID.triples, _void.createTypedLiteral(triples));

        Calendar created = getMetadataCreated();
        if (created != null)
            dataset.addProperty(DCTerms.created, _void.createTypedLiteral(created));

        Calendar modified = getMetadataModified();
        if (modified != null)
            dataset.addProperty(DCTerms.modified, _void.createTypedLiteral(modified));

        Calendar cal = Calendar.getInstance();
        Resource datasetDescription = _void.createResource(derefGraphUri, VOID.DatasetDescription)
                .addProperty(DCTerms.title, "Description of the dataset " + getName() + ".")
                .addProperty(RDFS.label, "Description of the dataset " + getName() + ".")
                .addProperty(FOAF.primaryTopic, dataset)
                .addProperty(DCTerms.modified, _void.createTypedLiteral(cal));

        return _void;
    }
}
