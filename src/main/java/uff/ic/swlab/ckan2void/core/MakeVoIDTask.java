package uff.ic.swlab.ckan2void.core;

import javax.naming.InvalidNameException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import uff.ic.swlab.ckan2void.util.Config;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.helper.VoIDHelper;
import uff.ic.swlab.ckan2void.util.SWLabHost;

public class MakeVoIDTask implements Runnable {

    private final Dataset dataset;
    private final SWLabHost server;
    private final String graphUri;
    private final String graphDerefUri;

    private static final InstanceCounter INSTANCE_COUNTER = new InstanceCounter(Config.TASK_INSTANCES);

    private static class InstanceCounter {

        private int instances;

        public InstanceCounter(int instances) {
            this.instances = instances;
        }

        public synchronized void startInstance() {
            while (true)
                if (instances > 0) {
                    instances--;
                    break;
                } else
                    try {
                        wait();
                    } catch (InterruptedException ex) {
                    }
        }

        public synchronized void finilizeInstance() {
            instances++;
            notifyAll();
        }
    }

    public MakeVoIDTask(Dataset dataset, String graphURI, SWLabHost server) {
        INSTANCE_COUNTER.startInstance();
        this.dataset = dataset;
        this.server = server;
        this.graphUri = graphURI;
        this.graphDerefUri = server.getQuadsURL(Config.FUSEKI_DATASET) + "?graph=" + graphUri;
    }

    @Override
    public final void run() {
        runTask();
        INSTANCE_COUNTER.finilizeInstance();
    }

    private void runTask() {
        try {
            String[] urls = dataset.getURLs(dataset);
            String[] sparqlEndPoints = dataset.getSparqlEndPoints();

            Model void_ = dataset.makeVoID(graphDerefUri);
            Model void__ = VoIDHelper.getContent(urls, sparqlEndPoints);
            saveVoid(void_, void__);

        } catch (Throwable e) {
            Logger.getLogger("error").log(Level.ERROR, String.format("Task error (<%1s>). Msg: %2s", graphUri, e.getMessage()));
        }
    }

    private void saveVoid(Model void_, Model void__) throws InvalidNameException {
        if (void_.size() == 0)
            Logger.getLogger("info").log(Level.INFO, String.format("Empty default VoID (<%1s>).", graphUri));
        if (void__.size() == 0)
            Logger.getLogger("info").log(Level.INFO, String.format("Empty crawled VoID (<%1s>).", graphUri));

        Model partitions;
        try {
            partitions = VoIDHelper.extractPartitions(void_);
        } catch (Throwable e) {
            partitions = ModelFactory.createDefaultModel();
        }

        if (partitions.size() == 0)
            void_.add(server.getModel(Config.FUSEKI_TEMP_DATASET, graphUri + "-partitions"));
        else
            server.putModel(Config.FUSEKI_TEMP_DATASET, graphUri + "-partitions", partitions);

        if (void__.size() == 0)
            void__ = server.getModel(Config.FUSEKI_TEMP_DATASET, graphUri);
        else
            server.putModel(Config.FUSEKI_TEMP_DATASET, graphUri, void__);

        if (void_.add(void__).size() > 5)
            server.putModel(Config.FUSEKI_DATASET, graphUri, void_);
        else
            Logger.getLogger("info").log(Level.INFO, String.format("Dataset discarded (<%1s>).", graphUri));
    }
}
