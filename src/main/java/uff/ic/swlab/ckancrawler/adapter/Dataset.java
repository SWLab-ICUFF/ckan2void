package uff.ic.swlab.ckancrawler.adapter;

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
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.WebContent;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.DCTerms;
import uff.ic.swlab.ckancrawler.Config;
import uff.ic.swlab.ckancrawler.util.Executor;
import uff.ic.swlab.ckancrawler.helper.URLHelper;

public class Dataset {

    private final CkanDataset doc;
    private final CkanClient cc;

    public Dataset(CkanClient cc, CkanDataset doc) {
        this.doc = doc;
        this.cc = cc;
    }

    public String getName() {
        try {
            return doc.getName();
        } catch (Throwable e) {
            return null;
        }
    }

    public String getNamespace() {
        try {
            return cc.getCatalogUrl() + "/api/rest/dataset/";
        } catch (Throwable e) {
            return null;
        }
    }

    public String getUri() {
        try {
            return getNamespace() + doc.getName();
        } catch (Throwable e) {
            return null;
        }
    }

    public String getJsonMetadataUrl() {
        try {
            return cc.getCatalogUrl() + "/api/rest/dataset/" + doc.getName();
        } catch (Throwable e) {
            return null;
        }
    }

    public String getWebMetadataUrl() {
        try {
            return cc.getCatalogUrl() + "/dataset/" + doc.getName();
        } catch (Throwable e) {
            return null;
        }
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

    public String[] getURLs(Dataset dataset) {
        String[] urls;
        Set<String> set = new HashSet<>();
        set.add(dataset.getUrl());
        set.addAll(Arrays.asList(dataset.getNamespaces()));
        set.addAll(Arrays.asList(dataset.getVoIDUrls()));
        set.addAll(Arrays.asList(dataset.getExampleUrls()));
        set.addAll(Arrays.asList(dataset.getDumpUrls()));
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
        try {
            for (CkanDatasetRelationship rel : doc.getRelationshipsAsSubject())
                links.put(cc.getDataset(rel.getObject()).getName(), Integer.parseInt(rel.getComment()));
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
                    try (QueryExecution exec = new QueryEngineHTTP(sparqlEndPoint, queryString)) {
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
                return Executor.execute(task, Config.SPARQL_TIMEOUT).entrySet();
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
                    try (QueryExecution exec = new QueryEngineHTTP(sparqlEndPoint, queryString)) {
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
                return Executor.execute(task, Config.SPARQL_TIMEOUT).entrySet();
            } catch (InterruptedException e) {
            } catch (Throwable e) {
            }
        return (new HashMap<String, Integer>()).entrySet();
    }

    public Model makeVoID(String graphDerefUri) {
        Model void_ = ModelFactory.createDefaultModel();

        Resource voidDatasetDescription = void_.createResource("http://rdfs.org/ns/void#DatasetDescription");
        Resource voidDataset = void_.createResource("http://rdfs.org/ns/void#Dataset");
        Resource voidLinkset = void_.createResource("http://rdfs.org/ns/void#Linkset");

        Property voidUriSpace = void_.createProperty("http://rdfs.org/ns/void#uriSpace");
        Property voidDataDump = void_.createProperty("http://rdfs.org/ns/void#dataDump");
        Property voidSparlEndpoint = void_.createProperty("http://rdfs.org/ns/void#sparqlEndpoint");
        Property voidSubset = void_.createProperty("http://rdfs.org/ns/void#subset");
        Property voidSubjectsTarget = void_.createProperty("http://rdfs.org/ns/void#sujectsTarget");
        Property voidObjectstarget = void_.createProperty("http://rdfs.org/ns/void#objectsTarget");
        Property voidClassPartition = void_.createProperty("http://rdfs.org/ns/void#classPartition");
        Property voidClass = void_.createProperty("http://rdfs.org/ns/void#class");
        Property voidEntities = void_.createProperty("http://rdfs.org/ns/void#entities");
        Property voidPropertyPartition = void_.createProperty("http://rdfs.org/ns/void#propertyPartition");
        Property voidProperty = void_.createProperty("http://rdfs.org/ns/void#property");
        Property voidTriples = void_.createProperty("http://rdfs.org/ns/void#triples");

        Resource dataset = void_.createResource(getUri(), voidDataset);
        Set<Entry<String, Integer>> links = getLinks();
        links.addAll(getLinks2());
        links.stream().forEach((link) -> {
            Resource linkset = void_.createResource(null, voidLinkset)
                    .addProperty(voidSubjectsTarget, dataset)
                    .addProperty(voidObjectstarget, void_.createResource(link.getKey()))
                    .addLiteral(voidTriples, void_.createTypedLiteral(link.getValue()));
            dataset.addProperty(voidSubset, linkset);
        });

        Set<Entry<String, Integer>> classes = getClasses();
        classes.stream().forEach((class_) -> {
            dataset.addProperty(voidClassPartition, void_.createResource()
                    .addProperty(voidClass, class_.getKey())
                    .addLiteral(voidEntities, void_.createTypedLiteral(class_.getValue())));
        });

        Set<Entry<String, Integer>> properties = getProperties();
        properties.stream().forEach((property) -> {
            dataset.addProperty(voidPropertyPartition, void_.createResource()
                    .addProperty(voidProperty, property.getKey())
                    .addLiteral(voidTriples, void_.createTypedLiteral(property.getValue())));
        });

        List<String> sparqlEndpoints = Arrays.asList(getSparqlEndPoints());
        sparqlEndpoints.stream().forEach((sparqlEndpoint) -> {
            dataset.addProperty(voidSparlEndpoint, void_.createResource(sparqlEndpoint));
        });

        List<String> dumps = Arrays.asList(getDumpUrls());
        dumps.stream().forEach((dumpURL) -> {
            dataset.addProperty(voidDataDump, void_.createResource(dumpURL));
        });

        List<String> uriSpaces = Arrays.asList(getNamespaces());
        uriSpaces.stream().forEach((uriSpace) -> {
            dataset.addProperty(voidUriSpace, uriSpace);
        });

        List<String> tags = Arrays.asList(getTags());
        tags.stream().forEach((tag) -> {
            dataset.addProperty(FOAF.topic, tag);
        });

        String title = getTitle();
        if (title != null && !title.equals(""))
            dataset.addProperty(DCTerms.title, title);

        String description = getNotes();
        if (description != null && !description.equals(""))
            dataset.addProperty(DCTerms.description, description);

        String homepage = getUrl();
        if (homepage != null && !homepage.equals(""))
            dataset.addProperty(FOAF.homepage, void_.createResource(homepage));

        String page1 = getJsonMetadataUrl();
        if (page1 != null && !page1.equals(""))
            dataset.addProperty(FOAF.page, void_.createResource(page1));

        String page2 = getWebMetadataUrl();
        if (page2 != null && !page2.equals(""))
            dataset.addProperty(FOAF.page, void_.createResource(page2));

        Integer triples = getTriples();
        if (triples != null)
            dataset.addProperty(voidTriples, void_.createTypedLiteral(triples));

        Calendar created = getMetadataCreated();
        if (created != null)
            dataset.addProperty(DCTerms.created, void_.createTypedLiteral(created));

        Calendar modified = getMetadataModified();
        if (modified != null)
            dataset.addProperty(DCTerms.modified, void_.createTypedLiteral(modified));

        Calendar cal = Calendar.getInstance();
        Resource datasetDescription = void_.createResource(graphDerefUri, voidDatasetDescription)
                .addProperty(DCTerms.title, "A VoID Description of the " + getName() + " dataset.")
                .addProperty(FOAF.primaryTopic, dataset)
                .addProperty(DCTerms.created, void_.createTypedLiteral(cal))
                .addProperty(DCTerms.modified, void_.createTypedLiteral(cal));

        return void_;
    }
}
