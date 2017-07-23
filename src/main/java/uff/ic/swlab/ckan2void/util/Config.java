package uff.ic.swlab.ckan2void.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.jena.sdb.StoreDesc;

public class Config {

    private SWLabHost host;

    private String fusekiDataset;
    private String fusekiTemDataset;
    private String datasetname;

    private String rdfRoot;

    private String localdatasetHomepageName;
    private String localNquadsDumpNamed;

    private String username;
    private String password;

    private String remoteDatasetHomepageName;
    private String remoteNquadsDumpName;

    private String ckanCatalogs;

    private Integer parallelism;
    private Integer taskInstances;
    private Integer poolShutdownTimeout;
    private TimeUnit poolShutdownTimeoutUnit;

    private Integer taskTimeout;
    private Integer sparqlTimeout;
    private Integer modelReadTimeout;
    private Integer modelWriteTimeout;
    private Integer httpConnectTimeout;
    private Integer httpReadTimeout;
    private Integer httpAccessTimeout;

    private Long maxVoidFileSize;

    private StoreDesc datasetDesc;
    private StoreDesc tempDatasetDesc;

    private Config() {
        try (InputStream input = new FileInputStream("./conf/ckan2void.properties");) {
            Properties prop = new Properties();
            prop.load(input);

            ckanCatalogs = prop.getProperty("ckanCatalog", "http://datahub.io");

            parallelism = Integer.valueOf(prop.getProperty("parallelism", "4"));
            taskInstances = (int) (parallelism * 1.1);

            poolShutdownTimeout = Integer.valueOf(prop.getProperty("poolShutdownTimeout", "1"));
            poolShutdownTimeoutUnit = TimeUnit.valueOf(prop.getProperty("poolShutdownTimeoutUnit", "HOURS"));
            taskTimeout = Integer.valueOf(prop.getProperty("taskTimeout", "300000"));
            sparqlTimeout = Integer.valueOf(prop.getProperty("sparqlTimeout", "60000"));
            modelReadTimeout = Integer.valueOf(prop.getProperty("modelReadTimeout", "60000"));
            modelWriteTimeout = Integer.valueOf(prop.getProperty("modelWriteTimeout", "60000"));
            httpConnectTimeout = Integer.valueOf(prop.getProperty("httpConnectTimeout", "30000"));
            httpReadTimeout = Integer.valueOf(prop.getProperty("httpReadTimeout", "30000"));
            httpAccessTimeout = Integer.valueOf(prop.getProperty("httpAccessTimeout", "60000"));

            maxVoidFileSize = Long.valueOf(prop.getProperty("maxVoidFileSize", "1048576"));

        } catch (Throwable t) {
            ckanCatalogs = "http://datahub.io";

            parallelism = 4;
            taskInstances = (int) (parallelism * 1.1);

            poolShutdownTimeout = 1;
            poolShutdownTimeoutUnit = TimeUnit.valueOf("HOURS");
            taskTimeout = 300000;
            sparqlTimeout = 60000;
            modelReadTimeout = 60000;
            modelWriteTimeout = 60000;
            httpConnectTimeout = 30000;
            httpReadTimeout = 30000;
            httpAccessTimeout = 60000;

            maxVoidFileSize = 1048576l;
        }

        try (InputStream input = new FileInputStream("./conf/auth.properties");) {
            Properties prop = new Properties();
            prop.load(input);

            username = prop.getProperty("username", "");
            password = prop.getProperty("password", "");

            String _host = prop.getProperty("host", "alternate");
            if (_host == null)
                host = SWLabHost.ALTERNATE_HOST;
            else if (_host.toLowerCase().equals("primary"))
                host = SWLabHost.PRIMARY_HOST;
            else if (_host.toLowerCase().equals("development"))
                host = SWLabHost.DEVELOPMENT_HOST;
            else if (_host.toLowerCase().equals("alternate"))
                host = SWLabHost.ALTERNATE_HOST;
            else
                host = SWLabHost.ALTERNATE_HOST;
        } catch (Throwable t) {
            username = "";
            password = "";
            host = SWLabHost.ALTERNATE_HOST;
        }

        fusekiDataset = "DatasetDescriptions";
        fusekiTemDataset = "temp";
        datasetname = fusekiDataset + "_v1";

        rdfRoot = "./data/v1/rdf";

        localdatasetHomepageName = rdfRoot + "/dataset/" + datasetname + "/index.jsp";
        localNquadsDumpNamed = rdfRoot + "/dataset/" + datasetname + ".nq.gz";

        remoteDatasetHomepageName = "/tomcat/dataset/" + datasetname + "/index.jsp";
        remoteNquadsDumpName = "/tomcat/dataset/" + datasetname + ".nq.gz";

        datasetDesc = StoreDesc.read("./conf/sdb1.ttl");
        tempDatasetDesc = StoreDesc.read("./conf/sdb2.ttl");
    }

    private static Config config;

    public static Config getInsatnce() {
        if (config == null)
            config = new Config();
        return config;
    }

    public String fusekiDataset() {
        return fusekiDataset;
    }

    public String fusekiTemDataset() {
        return fusekiTemDataset;
    }

    public String datasetname() {
        return datasetname;
    }

    public String rdfRoot() {
        return rdfRoot;
    }

    public String localDatasetHomepageName() {
        return localdatasetHomepageName;
    }

    public String localNquadsDumpName() {
        return localNquadsDumpNamed;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public String remoteDatasetHomepageName() {
        return remoteDatasetHomepageName;
    }

    public String remoteNquadsDumpName() {
        return remoteNquadsDumpName;
    }

    public String ckanCatalogs() {
        return ckanCatalogs;
    }

    public Integer parallelism() {
        return parallelism;
    }

    public Integer taskInstances() {
        return taskInstances;
    }

    public Integer poolShutdownTimeout() {
        return poolShutdownTimeout;
    }

    public TimeUnit poolShutdownTimeoutUnit() {
        return poolShutdownTimeoutUnit;
    }

    public Integer taskTimeout() {
        return taskTimeout;
    }

    public Integer sparqlTimeout() {
        return sparqlTimeout;
    }

    public Integer modelReadTimeout() {
        return modelReadTimeout;
    }

    public Integer modelWriteTimeout() {
        return modelWriteTimeout;
    }

    public Integer httpConnectTimeout() {
        return httpConnectTimeout;
    }

    public Integer httpReadTimeout() {
        return httpReadTimeout;
    }

    public Integer httpAccessTimeout() {
        return httpAccessTimeout;
    }

    public Long maxVoidFileSize() {
        return maxVoidFileSize;
    }

    public SWLabHost host() {
        return host;
    }

    public StoreDesc datasetDesc() {
        return datasetDesc;
    }

    public StoreDesc tempDatasetDesc() {
        return tempDatasetDesc;
    }
}
