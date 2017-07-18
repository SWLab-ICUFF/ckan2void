package uff.ic.swlab.ckan2void.util;

import java.io.IOException;
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
import uff.ic.swlab.ckan2void.helper.URLHelper;

public abstract class RDFDataMgr {

    public static Dataset loadDataset(String sparqlEndPoint, String query) throws TimeoutException, InterruptedException, ExecutionException {
        Callable<Dataset> task = () -> {
            Model tempModel = ModelFactory.createDefaultModel();
            try (final QueryExecution exec = new QueryEngineHTTP(sparqlEndPoint, query, HttpClients.createDefault())) {
                ((QueryEngineHTTP) exec).setModelContentType(WebContent.contentTypeRDFXML);
                ((QueryEngineHTTP) exec).setTimeout(Config.SPARQL_TIMEOUT);
                exec.execConstruct(tempModel);
                return DatasetFactory.create(tempModel);
            }
        };
        return Executor.execute(task, "Query dataset from " + sparqlEndPoint, Config.SPARQL_TIMEOUT);
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

    private static Dataset loadRDFDataset(String url, Lang lang, Long maxFileSize) throws InterruptedException, MalformedURLException, IOException, ExecutionException, TimeoutException, SizeLimitExceededException {
        Callable<Dataset> task = () -> {
            URLConnection conn = (new URL(url)).openConnection();
            conn.setConnectTimeout(Config.HTTP_CONNECT_TIMEOUT);
            conn.setReadTimeout(Config.HTTP_READ_TIMEOUT);
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
        return Executor.execute(task, "Load dataset from " + url + " using lang = " + lang.getName(), Config.HTTP_ACCESS_TIMEOUT);
    }

    private static Dataset loadRDFaDataset(String url, Long maxFileSize) throws UnsupportedEncodingException, MalformedURLException, IOException, InterruptedException, ExecutionException, TimeoutException, SizeLimitExceededException {
        Callable<Dataset> task = () -> {
            URL rdfaUrl = new URL(URLEncoder.encode("http://rdf-translator.appspot.com/convert/rdfa/xml/" + url, "UTF-8"));
            URLConnection conn = rdfaUrl.openConnection();
            conn.setConnectTimeout(Config.HTTP_CONNECT_TIMEOUT);
            conn.setReadTimeout(Config.HTTP_READ_TIMEOUT);
            if (conn.getContentLengthLong() <= maxFileSize)
                try (InputStream in = conn.getInputStream();) {
                    Model tempModel = ModelFactory.createDefaultModel();
                    org.apache.jena.riot.RDFDataMgr.read(tempModel, in, Lang.RDFXML);
                    return DatasetFactory.create(tempModel);
                }
            else
                throw new SizeLimitExceededException(String.format("Download size exceeded: %1s", url));
        };
        return Executor.execute(task, "Load RDFa dataset from " + url, Config.HTTP_ACCESS_TIMEOUT);
    }

//    public static void write(OutputStream output, Model model, Lang lang) {
//        if (lang == RDFLanguages.nameToLang("RDFa"))
//            write(output, model);
//        else if (lang == null)
//            writeHTML(output, model);
//    }
//    public static void write(OutputStream output, Model model) {
//        StringWriter writer = new StringWriter();
//        model.write(writer, Lang.RDFXML.getName());
//
//        HttpClient httpclient = new DefaultHttpClient();
//        HttpPost httppost = new HttpPost("http://rdf-translator.appspot.com/convert/detect/rdfa/content");
//        List<NameValuePair> params = new ArrayList<>(2);
//        params.add(new BasicNameValuePair("content", writer.toString()));
//        try {
//            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }
//
//        try {
//            HttpResponse response = httpclient.execute(httppost);
//            HttpEntity entity = response.getEntity();
//            if (entity != null) {
//                String content = EntityUtils.toString(entity);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//    private static void writeHTML(OutputStream output, Model model) {
//        String response = "";
//        response += "<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">";
//        response += "<html>";
//        response += "<head>";
//        response += "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=ISO-8859-1\">";
//        response += "<title>Entity description</title>";
//        response += "<link href=\"/swlab.css\" rel=\"stylesheet\" type=\"text/css\" />";
//        response += "</head>";
//        response += "<body>";
//        response += "<center>";
//        response += "<h1>Entity description</h1>";
//        response += "</center>";
//        response += "<br>";
//        response += "<br>";
//        response += "<br>";
//
//        response += "<table align=\"center\" border=\"0\">\n";
//        response += "<tr>\n";
//        response += "<td>\n";
//        response += "<center><h2>Satements</h2></center>\n";
//
//        response += "<table cellpadding=\"5\" align=\"center\" border=\"2\" style=\"word-wrap:break-word; table-layout: fixed; width: 80%;\">\n";
//        response += "<tr><th>subject</th><th>predicate</th><th>object</th></tr>\n";
//        StmtIterator iter = model.listStatements();
//        while (iter.hasNext()) {
//            Statement stmt = iter.nextStatement();
//            Resource subject = stmt.getSubject();
//            Property predicate = stmt.getPredicate();
//            RDFNode object = stmt.getObject();
//
//            String data1 = "<td><a href=\"%1s\">%2s</a></td>\n";
//            String data2 = "<td><a href=\"%1s\">%1s</a></td>\n";
//            String data3;
//            String data31 = "<td><a href=\"%1s\">%2s</a></td>\n";
//            String data32 = "<td>%1s</td>\n";
//            data1 = String.format(data1, subject.toString(), subject.toString());
//            data2 = String.format(data2, predicate.toString(), predicate.toString());
//            if (object instanceof Resource)
//                data3 = String.format(data31, object.toString(), object.toString());
//            else
//                data3 = String.format(data32, " \"" + object.toString() + "\"");
//
//            response += "<tr>\n";
//            response += data1 + data2 + data3;
//            response += "</tr>\n";
//        }
//        response += "</table>\n";
//
//        response += "</td>\n";
//        response += "</tr>\n";
//        response += "</table>\n";
//
//        response += "</body>";
//        response += "</html>";
//
//        try {
//            OutputStreamWriter writer = new OutputStreamWriter(output);
//            writer.write(response);
//            writer.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
}
