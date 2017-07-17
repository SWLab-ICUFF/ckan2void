package uff.ic.swlab.ckan2void.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public abstract class Config {

    public static final SWLabHost HOST = SWLabHost.DEFAULT_HOST;

    public static String FUSEKI_DATASET = "DatasetDescriptions";
    public static String FUSEKI_TEMP_DATASET = "temp";
    public static String DATASET_NAME = FUSEKI_DATASET + "_v1";

    public static final String RDF_ROOT = "./data/v1/rdf";

    public static String LOCAL_DATASET_HOMEPAGE = RDF_ROOT + "/dataset/" + DATASET_NAME + "/index.jsp";
    public static String LOCAL_NQUADS_DUMP_NAME = RDF_ROOT + "/dataset/" + DATASET_NAME + ".nq.gz";
    public static String LOCAL_TRIG_DUMP_NAME = RDF_ROOT + "/dataset/" + DATASET_NAME + ".trig.gz";
    public static String LOCAL_TRIX_DUMP_NAME = RDF_ROOT + "/dataset/" + DATASET_NAME + ".trix.gz";
    public static String LOCAL_JSONLD_DUMP_NAME = RDF_ROOT + "/dataset/" + DATASET_NAME + ".jsonld.gz";

    public static String USERNAME = null;
    public static String PASSWORD = null;

    public static String REMOTE_DATASET_HOMEPAGE = "/tomcat/dataset/" + DATASET_NAME + "/index.jsp";
    public static String REMOTE_NQUADS_DUMP_NAME = "/tomcat/dataset/" + DATASET_NAME + ".nq.gz";
    public static String REMOTE_TRIG_DUMP_NAME = "/tomcat/dataset/" + DATASET_NAME + ".trig.gz";
    public static String REMOTE_TRIX_DUMP_NAME = "/tomcat/dataset/" + DATASET_NAME + ".trix.gz";
    public static String REMOTE_JSONLD_DUMP_NAME = "/tomcat/dataset/" + DATASET_NAME + ".jsonld.gz";

    public static String CKAN_CATALOGS;

    public static Integer TASK_INSTANCES;
    public static Integer PARALLELISM;
    public static Integer POOL_SHUTDOWN_TIMEOUT;
    public static TimeUnit POOL_SHUTDOWN_TIMEOUT_UNIT;

    public static Integer TASK_TIMEOUT;
    public static Integer SPARQL_TIMEOUT;
    public static Integer MODEL_READ_TIMEOUT;
    public static Integer MODEL_WRITE_TIMEOUT;
    public static Integer HTTP_CONNECT_TIMEOUT;
    public static Integer HTTP_READ_TIMEOUT;
    public static Integer HTTP_ACCESS_TIMEOUT;

    public static Long MAX_VOID_FILE_SIZE;

    public static void configure(String filename) throws IOException {
        try (InputStream input = new FileInputStream(filename);) {
            Properties prop = new Properties();
            prop.load(input);

            CKAN_CATALOGS = prop.getProperty("ckanCatalog", "http://datahub.io");

            TASK_INSTANCES = Integer.valueOf(prop.getProperty("taskInstances", "8"));
            PARALLELISM = Integer.valueOf(prop.getProperty("parallelism", "4"));
            POOL_SHUTDOWN_TIMEOUT = Integer.valueOf(prop.getProperty("poolShutdownTimeout", "1"));
            POOL_SHUTDOWN_TIMEOUT_UNIT = TimeUnit.valueOf(prop.getProperty("poolShutdownTimeoutUnit", "HOURS"));

            TASK_TIMEOUT = Integer.valueOf(prop.getProperty("taskTimeout", "300000"));
            SPARQL_TIMEOUT = Integer.valueOf(prop.getProperty("sparqlTimeout", "60000"));
            MODEL_READ_TIMEOUT = Integer.valueOf(prop.getProperty("modelReadTimeout", "60000"));
            MODEL_WRITE_TIMEOUT = Integer.valueOf(prop.getProperty("modelWriteTimeout", "60000"));
            HTTP_CONNECT_TIMEOUT = Integer.valueOf(prop.getProperty("httpConnectTimeout", "30000"));
            HTTP_READ_TIMEOUT = Integer.valueOf(prop.getProperty("httpReadTimeout", "30000"));
            HTTP_ACCESS_TIMEOUT = Integer.valueOf(prop.getProperty("httpAccessTimeout", "60000"));

            MAX_VOID_FILE_SIZE = Long.valueOf(prop.getProperty("maxVoidFileSize", "1048576"));
        }
    }

    public static void configureAuth(String filename) throws IOException {
        try (InputStream input = new FileInputStream(filename);) {
            Properties prop = new Properties();
            prop.load(input);

            USERNAME = prop.getProperty("username");
            PASSWORD = prop.getProperty("password");
        }
    }
}
