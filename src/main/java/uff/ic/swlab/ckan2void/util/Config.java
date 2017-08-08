package uff.ic.swlab.ckan2void.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Config {

    private SWLabHost host;

    private String fusekiDataset = "DatasetDescriptions";
    private String fusekiTemDataset = "temp";
    private String datasetname = fusekiDataset + "_v1";

    private String htmlRootDir = "./data/v1/html";
    private String rdfRootDir = "./data/v1/rdf";

    private String localdatasetHomepageName = htmlRootDir + "/dataset/" + datasetname + "/index.jsp";
    private String localNquadsDumpNamed = rdfRootDir + "/dataset/" + datasetname + ".nq.gz";

    private String username;
    private String password;

    private String remoteDatasetHomepageName = "/tomcat/dataset/" + datasetname + "/index.jsp";
    private String remoteNquadsDumpName = "/tomcat/dataset/" + datasetname + ".nq.gz";

    private String ckanCatalogs = "http://datahub.io";

    private Integer parallelism;
    private Integer taskInstances;
    private Integer poolShutdownTimeout;
    private TimeUnit poolShutdownTimeoutUnit;

    private Integer taskTimeout;
    private Integer saveTimeout;
    private Integer sparqlTimeout;
    private Integer modelReadTimeout;
    private Integer modelWriteTimeout;
    private Integer httpConnectTimeout;
    private Integer httpReadTimeout;
    private Integer httpAccessTimeout;

    private Long maxVoidFileSize;

    private String datasetSDBDesc;
    private String tempDatasetSDBDesc;

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
            saveTimeout = Integer.valueOf(prop.getProperty("saveTimeout", "300000"));
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

        try (InputStream input = new FileInputStream("./conf/host.properties");) {
            Properties prop = new Properties();
            prop.load(input);

            String hostname = prop.getProperty("hostname", "localhost");
            int httpPort = Integer.parseInt(prop.getProperty("httpPort", "8080"));
            int ftpPort = Integer.parseInt(prop.getProperty("ftpPort", "2121"));
            host = new SWLabHost(hostname, httpPort, ftpPort);

            username = prop.getProperty("username", "");
            password = prop.getProperty("password", "");
        } catch (Throwable t) {
            username = "";
            password = "";
            host = new SWLabHost("localhost", 8080, 2121);
        }

        datasetSDBDesc = "./conf/sdb1.ttl";
        tempDatasetSDBDesc = "./conf/sdb2.ttl";
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
        return rdfRootDir;
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

    public Integer saveTimeout() {
        return saveTimeout;
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

    public String datasetSDBDesc() {
        return datasetSDBDesc;
    }

    public String tempDatasetSDBDesc() {
        return tempDatasetSDBDesc;
    }
}
