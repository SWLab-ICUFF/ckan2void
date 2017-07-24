package uff.ic.swlab.ckan2void.core;

import java.util.concurrent.Callable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.helper.VoIDHelper;
import uff.ic.swlab.ckan2void.util.Config;
import uff.ic.swlab.ckan2void.util.Executor;

public class MakeVoIDTask implements Runnable {

    private Dataset dataset;
    private String graphUri;
    private Config conf;

    private static InstanceCounter counter;

    private static class InstanceCounter {

        private int instances;

        public InstanceCounter(int instances) {
            this.instances = instances;
        }

        public synchronized void newInstance() {
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

        public synchronized void finalizeInstance() {
            instances++;
            notifyAll();
        }
    }

    public MakeVoIDTask(Dataset dataset, String graphURI) {
        conf = Config.getInsatnce();
        if (counter == null)
            counter = new InstanceCounter(conf.taskInstances());
        counter.newInstance();

        this.dataset = dataset;
        this.graphUri = graphURI;
    }

    @Override
    public final void run() {
        runTask();
        counter.finalizeInstance();
    }

    private void runTask() {
        try {
            Model _void = ModelFactory.createDefaultModel();
            Model _voidComp = ModelFactory.createDefaultModel();

            Callable<Object> task = () -> {
                String[] urls = dataset.getURLs();
                String[] sparqlEndPoints = dataset.getSparqlEndPoints();
                _void.add(dataset.toVoid());
                _voidComp.add(VoIDHelper.getContent(urls, sparqlEndPoints, dataset.getUri()));
                return null;
            };
            Executor.execute(task, "Make void of " + dataset.getUri(), conf.taskTimeout());
            conf.host().saveVoid(_void, _voidComp, dataset.getUri(), graphUri);

        } catch (Throwable e) {
            Logger.getLogger("error").log(Level.ERROR, String.format("Task failure (<%1$s>). Msg: %2$s", graphUri, e.getMessage()));
        }

    }

}
