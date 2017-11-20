package uff.ic.swlab.ckan2void.util;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.naming.SizeLimitExceededException;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.WebContent;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;

public abstract class RDFDataMgr {

    public static Dataset loadDataset(String sparqlEndPoint, String query) throws TimeoutException, InterruptedException, ExecutionException {
        Callable<Dataset> task = () -> {
            Model tempModel = ModelFactory.createDefaultModel();
            try (final QueryExecution exec = new QueryEngineHTTP(sparqlEndPoint, query, HttpClients.createDefault())) {
                ((QueryEngineHTTP) exec).setModelContentType(WebContent.contentTypeRDFXML);
                ((QueryEngineHTTP) exec).setTimeout(Config.getInsatnce().sparqlTimeout());
                exec.execConstruct(tempModel);
                return DatasetFactory.create(tempModel);
            }
        };
        return Executor.execute(task, "Query dataset from " + sparqlEndPoint, Config.getInsatnce().sparqlTimeout());
    }

    public static Dataset loadDataset(String url, Long maxFileSize) throws InterruptedException {
        try {
            Lang[] langs = {null, Lang.TURTLE, Lang.RDFXML, Lang.NTRIPLES, Lang.TRIG,
                Lang.NQUADS, Lang.JSONLD, Lang.RDFJSON, Lang.TRIX, Lang.RDFTHRIFT};

            if (URLHelper.isHTML(url))
                return loadRDFaDataset(url, maxFileSize);
            else
                for (Lang lang : langs)
                    try {
                        return loadRDFDataset(url, lang, maxFileSize);
                    } catch (InterruptedException e) {
                        throw e;
                    } catch (Throwable e) {
                    }
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable e) {
        }
        return DatasetFactory.create();
    }

    private static Dataset loadRDFDataset(String url, Lang lang, Long maxFileSize) throws InterruptedException, MalformedURLException, ExecutionException, TimeoutException, SizeLimitExceededException {
        Callable<Dataset> task = () -> {
            URLConnection conn = (new URL(url)).openConnection();
            conn.setConnectTimeout(Config.getInsatnce().httpConnectTimeout());
            conn.setReadTimeout(Config.getInsatnce().httpReadTimeout());
            if (conn.getContentLengthLong() <= maxFileSize)
                try (InputStream in = conn.getInputStream();) {

                    Dataset tempDataset = DatasetFactory.create();
                    if (lang == null)
                        org.apache.jena.riot.RDFDataMgr.read(tempDataset, url);
                    else
                        org.apache.jena.riot.RDFDataMgr.read(tempDataset, in, lang);
                    return tempDataset;
                }
            else
                throw new SizeLimitExceededException(String.format("Download size exceeded: %1s", url));
        };
        return Executor.execute(task, "Load dataset from " + url + " using lang = " + lang.getName(), Config.getInsatnce().httpAccessTimeout());
    }

    private static Dataset loadRDFaDataset(String url, Long maxFileSize) throws UnsupportedEncodingException, MalformedURLException, InterruptedException, ExecutionException, TimeoutException, SizeLimitExceededException {
        Callable<Dataset> task = () -> {
            URL rdfaUrl = new URL(URLEncoder.encode("http://rdf-translator.appspot.com/convert/rdfa/xml/" + url, "UTF-8"));
            URLConnection conn = rdfaUrl.openConnection();
            conn.setConnectTimeout(Config.getInsatnce().httpConnectTimeout());
            conn.setReadTimeout(Config.getInsatnce().httpReadTimeout());
            if (conn.getContentLengthLong() <= maxFileSize)
                try (InputStream in = conn.getInputStream();) {
                    Model tempModel = ModelFactory.createDefaultModel();
                    org.apache.jena.riot.RDFDataMgr.read(tempModel, in, Lang.RDFXML);
                    return DatasetFactory.create(tempModel);
                }
            else
                throw new SizeLimitExceededException(String.format("Download size exceeded: %1s", url));
        };
        return Executor.execute(task, "Load RDFa dataset from " + url, Config.getInsatnce().httpAccessTimeout());
    }
}
