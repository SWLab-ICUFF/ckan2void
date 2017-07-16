package uff.ic.swlab.ckan2void.core;

import javax.naming.InvalidNameException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.helper.VoIDHelper;
import uff.ic.swlab.ckan2void.util.Config;

public class MakeVoIDTask implements Runnable {

    private final Dataset dataset;
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

    public MakeVoIDTask(Dataset dataset, String graphURI) {
        INSTANCE_COUNTER.startInstance();
        this.dataset = dataset;
        this.graphUri = graphURI;
        this.graphDerefUri = Config.HOST.getQuadsURL(Config.FUSEKI_DATASET) + "?graph=" + graphUri;
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

            Model _void = dataset.toVoid(graphDerefUri);
            Model _voidExtra = VoIDHelper.getContent(urls, sparqlEndPoints, dataset.getUri());
            saveVoid(_void, _voidExtra);

        } catch (Throwable e) {
            Logger.getLogger("error").log(Level.ERROR, String.format("Task error (<%1$s>). Msg: %2$s", graphUri, e.getMessage()));
        }
    }

    private void saveVoid(Model _void, Model _voidExtra) throws InvalidNameException {
        if (_void.size() == 0)
            Logger.getLogger("info").log(Level.INFO, String.format("Empty synthetized VoID (<%1s>).", graphUri));
        if (_voidExtra.size() == 0)
            Logger.getLogger("info").log(Level.INFO, String.format("Empty captured VoID (<%1s>).", graphUri));

        Model partitions;
        try {
            partitions = VoIDHelper.extractPartitions(_void, dataset.getUri());
        } catch (Throwable e) {
            partitions = ModelFactory.createDefaultModel();
        }

        if (partitions.size() == 0)
            _void.add(Config.HOST.getModel(Config.FUSEKI_TEMP_DATASET, graphUri + "-partitions"));
        else
            Config.HOST.putModel(Config.FUSEKI_TEMP_DATASET, graphUri + "-partitions", partitions);

        if (_voidExtra.size() == 0)
            _voidExtra = Config.HOST.getModel(Config.FUSEKI_TEMP_DATASET, graphUri);
        else
            Config.HOST.putModel(Config.FUSEKI_TEMP_DATASET, graphUri, _voidExtra);

        if (_void.add(_voidExtra).size() > 5)
            Config.HOST.putModel(Config.FUSEKI_DATASET, graphUri, _void);
        else
            Logger.getLogger("info").log(Level.INFO, String.format("Dataset discarded (<%1s>).", graphUri));
    }
}
