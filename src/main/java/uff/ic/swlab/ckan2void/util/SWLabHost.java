package uff.ic.swlab.ckan2void.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import javax.naming.InvalidNameException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.WebContent;
import org.apache.jena.sdb.SDBFactory;
import org.apache.jena.sdb.Store;
import org.apache.jena.sdb.StoreDesc;
import org.apache.jena.sdb.util.StoreUtils;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SWLabHost {

    public String hostname;
    public int httpPort;
    public int ftpPort;

    private SWLabHost() {

    }

    public SWLabHost(String hostname, int httpPort, int ftpPort) {
        this.hostname = hostname;
        this.httpPort = httpPort;
        this.ftpPort = ftpPort;
    }

    public String baseHttpUrl() {
        return "http://" + hostname + (httpPort == 80 ? "" : ":" + httpPort) + "/";
    }

    public String NS() {
        return baseHttpUrl() + "resource/";
    }

    public String getBackupURL(String datasetname) {
        return String.format(baseHttpUrl() + "$/backup/%1$s", datasetname);
    }

    public String getQuadsURL(String datasetname) {
        return String.format(baseHttpUrl() + "%1$s/", datasetname);
    }

    public String getSparqlURL(String datasetname) {
        return String.format(baseHttpUrl() + "%1$s/sparql", datasetname);
    }

    public String getUpdateURL(String datasetname) {
        return String.format(baseHttpUrl() + "%1$s/update", datasetname);
    }

    public String getDataURL(String datasetname) {
        return String.format(baseHttpUrl() + "%1$s/data", datasetname);
    }

    public String updateURL(String datasetname) {
        return String.format(baseHttpUrl() + "%1$s/update", datasetname);
    }

//    public synchronized List<String> listGraphNames(String datasetname, long timeout) {
//        List<String> graphNames = new ArrayList<>();
//
//        String queryString = "select distinct ?g where {graph ?g {[] ?p [].}}";
//        try (QueryExecution exec = new QueryEngineHTTP(getSparqlURL(datasetname), queryString, HttpClients.createDefault())) {
//            ((QueryEngineHTTP) exec).setTimeout(timeout);
//            ResultSet rs = exec.execSelect();
//            while (rs.hasNext())
//                graphNames.add(rs.next().getResource("g").getURI());
//        } catch (Exception e) {
//        }
//
//        return graphNames;
//    }
    public synchronized Model execConstruct(String queryString, String datasetname) {
        Model result = ModelFactory.createDefaultModel();
        try (final QueryExecution exec = new QueryEngineHTTP(getSparqlURL(datasetname), queryString, HttpClients.createDefault())) {
            ((QueryEngineHTTP) exec).setModelContentType(WebContent.contentTypeRDFXML);
            exec.execConstruct(result);
        }
        return result;
    }

    public synchronized void execUpdate(String queryString, String datasetname) {
        UpdateRequest request = UpdateFactory.create(queryString);
        UpdateProcessor execution = UpdateExecutionFactory.createRemote(request, getUpdateURL(datasetname));
        execution.execute();
    }

    public synchronized void execUpdate(String queryString, StoreDesc storeDesc) {
        Store datasetStore = SDBFactory.connectStore(storeDesc);
        try {
            Dataset dataset = SDBFactory.connectDataset(datasetStore);
            UpdateRequest request = UpdateFactory.create(queryString);
            UpdateProcessor execution = UpdateExecutionFactory.create(request, dataset);
            execution.execute();
        } finally {
            datasetStore.getConnection().close();
        }
    }

    public synchronized void backupDataset(String datasetname) throws Exception, IOException {
        System.out.println(String.format("Requesting backup of the Fuseki dataset %1$s...", datasetname));
        try {
            HttpClient httpclient = HttpClients.createDefault();
            HttpResponse response = httpclient.execute(new HttpPost(getBackupURL(datasetname)));
            int statuscode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity != null && statuscode == 200)
                try (final InputStream instream = entity.getContent()) {
                    System.out.println(IOUtils.toString(instream, "utf-8"));
                    System.out.println("Done.");
                }
            else
                System.out.println("Backup request failed.");
        } catch (Throwable e) {
            System.out.println("Backup request failed.");
        }
    }

    public synchronized void putModel(String datasetname, Model sourceModel) throws InvalidNameException {
        DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(getDataURL(datasetname), HttpClients.createDefault());
        accessor.putModel(sourceModel);
    }

    public synchronized void putModel(String datasetname, String graphUri, Model model) throws InvalidNameException {
        if (graphUri != null && !graphUri.equals("")) {
            DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(getDataURL(datasetname), HttpClients.createDefault());
            accessor.putModel(graphUri, model);
        } else
            throw new InvalidNameException(String.format("Invalid graph URI: %1$s.", graphUri));
    }

    public synchronized Model getModel(String datasetname, String graphUri) throws InvalidNameException {
        if (graphUri != null && !graphUri.equals("")) {
            DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(getDataURL(datasetname), HttpClients.createDefault());
            Model model = accessor.getModel(graphUri);
            if (model != null)
                return model;
            else
                return ModelFactory.createDefaultModel();
        } else
            throw new InvalidNameException(String.format("Invalid graph URI: %1$s.", graphUri));
    }

    public synchronized Model getModel(String datasetname) {
        DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(getDataURL(datasetname), HttpClients.createDefault());
        Model model = accessor.getModel();
        if (model != null)
            return model;
        else
            return ModelFactory.createDefaultModel();
    }

    public synchronized void saveVoid(Model _void, Model _voidComp, String datasetUri, String graphUri) throws InvalidNameException, SQLException {
        Store datasetStore = SDBFactory.connectStore(Config.getInsatnce().datasetSDBDesc());
        Store tempDatasetStore = SDBFactory.connectStore(Config.getInsatnce().tempDatasetSDBDesc());
        try {

            Model partitions;
            try {
                partitions = VoIDHelper.extractPartitions(_void, datasetUri);
            } catch (Throwable e) {
                partitions = ModelFactory.createDefaultModel();
            }

            datasetStore.getConnection().getTransactionHandler().begin();
            tempDatasetStore.getConnection().getTransactionHandler().begin();

            Dataset tempDataset = SDBFactory.connectDataset(tempDatasetStore);
            Dataset dataset = SDBFactory.connectDataset(datasetStore);

            if (partitions.size() == 0)
                _void.add(tempDataset.getNamedModel(graphUri + "-partitions"));
            else
                tempDataset.replaceNamedModel(graphUri + "-partitions", partitions);

            if (_voidComp.size() == 0)
                _voidComp = tempDataset.getNamedModel(graphUri);
            else
                tempDataset.replaceNamedModel(graphUri, _voidComp);

            if (_void.add(_voidComp).size() > 5) {
                dataset.replaceNamedModel(graphUri, _void);
                putModel(Config.getInsatnce().fusekiDataset(), graphUri, _void);

                tempDatasetStore.getConnection().getTransactionHandler().commit();
                datasetStore.getConnection().getTransactionHandler().commit();
                Logger.getLogger("info").log(Level.INFO, String.format("Dataset save commited (<%1$s>).", graphUri));
                if (_void.size() == 0)
                    Logger.getLogger("info").log(Level.INFO, String.format("Empty VoIDComp (<%1$s>).", datasetUri));
            } else {
                tempDatasetStore.getConnection().getTransactionHandler().abort();
                datasetStore.getConnection().getTransactionHandler().abort();
                Logger.getLogger("info").log(Level.INFO, String.format("Dataset discarded (<%1$s>).", graphUri));
            }

        } catch (Throwable t) {
            tempDatasetStore.getConnection().getTransactionHandler().abort();
            datasetStore.getConnection().getTransactionHandler().abort();
            throw t;
        } finally {
            tempDatasetStore.getConnection().close();
            datasetStore.getConnection().close();
        }
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

    public synchronized static void initSDB(String desc) throws SQLException {
        Store store = SDBFactory.connectStore(StoreDesc.read(desc));
        try {
            if (!StoreUtils.isFormatted(store))
                store.getTableFormatter().create();
        } finally {
            store.getConnection().close();
        }
    }

}
