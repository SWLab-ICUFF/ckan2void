package uff.ic.swlab.ckan2void.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.naming.InvalidNameException;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.WebContent;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public enum SWLabHost {

    PRIMARY_HOST("swlab.ic.uff.br", 80, 21),
    DEVELOPMENT_HOST("swlab.ic.uff.br", 8080, 2121),
    ALTERNATE_HOST("swlab.paes-leme.name", 8080, 2121);

    public final String hostname;
    public final int httpPort;
    public final int ftpPort;
    public static final SWLabHost DEFAULT_HOST = ALTERNATE_HOST;

    SWLabHost(String hostname, int httpPort, int ftpPort) {
        this.hostname = hostname;
        this.httpPort = httpPort;
        this.ftpPort = ftpPort;
    }

    public String baseHttpUrl() {
        return "http://" + hostname + (httpPort == 80 ? "" : ":" + httpPort) + "/";
    }

    public String linkedDataNS() {
        return baseHttpUrl() + "resource/";
    }

    public String getQuadsURL(String datasetname) {
        return String.format(baseHttpUrl() + "fuseki/%1s/", datasetname);
    }

    public String getSparqlURL(String datasetname) {
        return String.format(baseHttpUrl() + "fuseki/%1s/sparql", datasetname);
    }

    public String getDataURL(String datasetname) {
        return String.format(baseHttpUrl() + "fuseki/%1s/data", datasetname);
    }

    public String updateURL(String datasetname) {
        return String.format(baseHttpUrl() + "fuseki/%1s/update", datasetname);
    }

    public synchronized List<String> listGraphNames(String datasetname, long timeout) {
        List<String> graphNames = new ArrayList<>();

        String queryString = "select distinct ?g where {graph ?g {[] ?p [].}}";
        try (QueryExecution exec = new QueryEngineHTTP(getSparqlURL(datasetname), queryString)) {
            ((QueryEngineHTTP) exec).setTimeout(timeout);
            ResultSet rs = exec.execSelect();
            while (rs.hasNext())
                graphNames.add(rs.next().getResource("g").getURI());
        } catch (Exception e) {
        }

        return graphNames;
    }

    public synchronized void loadDataset(String datasetname, String uri) {
        Dataset ds = RDFDataMgr.loadDataset(uri);
        DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(getDataURL(datasetname));
        accessor.putModel(ds.getDefaultModel());
        Iterator<String> iter = ds.listNames();
        while (iter.hasNext()) {
            String graphUri = iter.next();
            accessor.putModel(graphUri, ds.getNamedModel(graphUri));
        }
    }

    public synchronized void putModel(String datasetname, String graphUri, Model model) throws InvalidNameException {
        if (graphUri != null && !graphUri.equals("")) {
            DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(getDataURL(datasetname));
            accessor.putModel(graphUri, model);
            Logger.getLogger("info").log(Level.INFO, String.format("Dataset saved (<%1s>).", graphUri));
            return;
        }
        throw new InvalidNameException(String.format("Invalid graph URI: %1s.", graphUri));
    }

    public synchronized Model getModel(String datasetname, String graphUri) throws InvalidNameException {
        if (graphUri != null && !graphUri.equals("")) {
            DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(getDataURL(datasetname));
            Model model = accessor.getModel(graphUri);
            if (model != null)
                return model;
            else
                return ModelFactory.createDefaultModel();
        }
        throw new InvalidNameException(String.format("Invalid graph URI: %1s.", graphUri));
    }

    public synchronized Model execConstruct(String queryString, String datasetname) {
        Model result = ModelFactory.createDefaultModel();
        try (final QueryExecution exec = new QueryEngineHTTP(getSparqlURL(datasetname), queryString)) {
            ((QueryEngineHTTP) exec).setModelContentType(WebContent.contentTypeRDFXML);
            exec.execConstruct(result);
        }
        return result;
    }

    public synchronized void uploadBinaryFile(String localFilename, String remoteName, String user, String pass) throws IOException, Exception {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(hostname, ftpPort);

        try (InputStream in = new FileInputStream(localFilename);) {
            String[] dirs = remoteName.split("/");
            if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode()))
                if (ftpClient.login(user, pass)) {
                    //ftpClient.execPBSZ(0);
                    //ftpClient.execPROT("P");
                    ftpClient.enterLocalPassiveMode();
                    //ftpClient.enterRemotePassiveMode();
                    ftpClient.mkd(String.join("/", Arrays.copyOfRange(dirs, 0, dirs.length - 1)));
                    ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                    ftpClient.storeFile(remoteName, in);
                    ftpClient.logout();
                }
            ftpClient.disconnect();
        } catch (IOException e) {
            try {
                ftpClient.disconnect();
            } catch (IOException e2) {
            }
            throw e;
        }
    }

    public synchronized void mkDirsViaFTP(String remoteFilename, String user, String pass) throws Exception {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(hostname, ftpPort);

        try {
            if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode()))
                if (ftpClient.login(user, pass)) {
                    String[] dirs = remoteFilename.split("/");
                    //ftpClient.execPBSZ(0);
                    //ftpClient.execPROT("P");
                    ftpClient.enterLocalPassiveMode();
                    //ftpClient.enterRemotePassiveMode();
                    ftpClient.mkd(String.join("/", Arrays.copyOfRange(dirs, 0, dirs.length - 1)));
                    ftpClient.logout();
                }
            ftpClient.disconnect();
        } catch (IOException e) {
            try {
                ftpClient.disconnect();
            } catch (IOException e2) {
            }
            throw e;
        }
    }
}
