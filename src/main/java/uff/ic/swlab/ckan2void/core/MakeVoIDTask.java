package uff.ic.swlab.ckan2void.core;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
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
    private String datasetUri;
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

    public MakeVoIDTask(Dataset dataset, String graphURI) throws InterruptedException, TimeoutException, ExecutionException {
        conf = Config.getInsatnce();
        if (counter == null)
            counter = new InstanceCounter(conf.taskInstances());
        counter.newInstance();

        this.dataset = dataset;
        this.datasetUri = dataset.getUri();
        this.graphUri = graphURI;
    }

    @Override
    public final void run() {
        runTask();
        counter.finalizeInstance();
    }

    private void runTask() {

        try {//make void

            Model _void = ModelFactory.createDefaultModel();
            Callable<Object> makeVoid = () -> {
                String[] urls = dataset.getURLs();
                String[] sparqlEndPoints = dataset.getSparqlEndPoints();
                _void.add(dataset.toVoid());
                return null;
            };
            Executor.execute(makeVoid, "make VoID of " + dataset.getUri(), conf.taskTimeout());

            try {// make void comp

                Model _voidComp = ModelFactory.createDefaultModel();
                Callable<Object> makeVoidComp = () -> {
                    String[] urls = dataset.getURLs();
                    String[] sparqlEndPoints = dataset.getSparqlEndPoints();
                    _voidComp.add(VoIDHelper.getContent(urls, sparqlEndPoints, conf.host().NS(), dataset.getUri()));
                    return null;
                };
                Executor.execute(makeVoidComp, "make VoIDComp of " + dataset.getUri(), conf.taskTimeout());

                try {// save void + voidComp

                    Callable<Object> save = () -> {
                        conf.host().saveVoid(_void, _voidComp, datasetUri, graphUri);
                        return null;
                    };
                    Executor.execute(save, "save VoID + VoIDComp of " + dataset.getUri(), conf.saveTimeout());

                } catch (Throwable e) {
                    Logger.getLogger("error").log(Level.ERROR, String.format("Save VoID + VoIDComp failure (<%1$s>). Msg: %2$s", datasetUri, e.getMessage()));

                    try {// save void + voidComp

                        Model emptyModel = ModelFactory.createDefaultModel();
                        Callable<Object> save = () -> {
                            conf.host().saveVoid(_void, emptyModel, datasetUri, graphUri);
                            return null;
                        };
                        Executor.execute(save, "Retry save VoID of " + dataset.getUri(), conf.saveTimeout());

                    } catch (Throwable e2) {
                        Logger.getLogger("error").log(Level.ERROR, String.format("Retry save VoID failure (<%1$s>). Msg: %2$s", datasetUri, e2.getMessage()));
                    }
                }
            } catch (Throwable e) {
                Logger.getLogger("error").log(Level.ERROR, String.format("Make VoIDComp failure (<%1$s>). Msg: %2$s", datasetUri, e.getMessage()));

                try {// save void

                    Model _voidComp = ModelFactory.createDefaultModel();
                    Callable<Object> save = () -> {
                        conf.host().saveVoid(_void, _voidComp, datasetUri, graphUri);
                        return null;
                    };
                    Executor.execute(save, "save VoID of " + dataset.getUri(), conf.saveTimeout());

                } catch (Throwable e2) {
                    Logger.getLogger("error").log(Level.ERROR, String.format("Save VoID failure (<%1$s>). Msg: %2$s", datasetUri, e2.getMessage()));
                }
            }
        } catch (Throwable e) {
            Logger.getLogger("error").log(Level.ERROR, String.format("Make VoID failure (<%1$s>). Msg: %2$s", datasetUri, e.getMessage()));
        }
    }

    private void runTask2() {
        Model _void = ModelFactory.createDefaultModel();
        Model _voidComp = ModelFactory.createDefaultModel();

        try {//make

            Callable<Object> makeVoid = () -> {
                String[] urls = dataset.getURLs();
                String[] sparqlEndPoints = dataset.getSparqlEndPoints();
                _void.add(dataset.toVoid());
                _voidComp.add(VoIDHelper.getContent(urls, sparqlEndPoints, conf.host().NS(), dataset.getUri()));
                return null;
            };
            Executor.execute(makeVoid, "make void of " + dataset.getUri(), conf.taskTimeout());

            try {//save

                Callable<Object> save = () -> {
                    conf.host().saveVoid(_void, _voidComp, datasetUri, graphUri);
                    return null;
                };
                Executor.execute(save, "save void of " + dataset.getUri(), conf.saveTimeout());

            } catch (TimeoutException | ExecutionException e) {

                try {//retry save

                    _voidComp.removeAll();
                    Callable<Object> task = () -> {
                        conf.host().saveVoid(_void, _voidComp, datasetUri, graphUri);
                        return null;
                    };
                    Executor.execute(task, "retry to save void of " + dataset.getUri(), conf.saveTimeout());

                } catch (Throwable e2) {
                    Logger.getLogger("error").log(Level.ERROR, String.format("Retry save VoID failure (<%1$s>). Msg: %2$s", datasetUri, e.getMessage()));
                }
            } catch (Throwable e) {
                Logger.getLogger("error").log(Level.ERROR, String.format("Save VoID failure (<%1$s>). Msg: %2$s", datasetUri, e.getMessage()));
            }

        } catch (Throwable e) {
            try {// retry make
                _void.removeAll();
                _voidComp.removeAll();

                Callable<Object> makeVoid = () -> {
                    String[] urls = dataset.getURLs();
                    String[] sparqlEndPoints = dataset.getSparqlEndPoints();
                    _void.add(dataset.toVoid());
                    return null;
                };
                Executor.execute(makeVoid, "make void of " + dataset.getUri(), conf.taskTimeout());

                try {//save

                    Callable<Object> save = () -> {
                        conf.host().saveVoid(_void, _voidComp, datasetUri, graphUri);
                        return null;
                    };
                    Executor.execute(save, "save void of " + dataset.getUri(), conf.saveTimeout());

                } catch (Throwable e2) {
                    Logger.getLogger("error").log(Level.ERROR, String.format("Save retry VoID failure (<%1$s>). Msg: %2$s", datasetUri, e2.getMessage()));
                }
            } catch (Throwable e2) {
                Logger.getLogger("error").log(Level.ERROR, String.format("Retry make VoID failure (<%1$s>). Msg: %2$s", datasetUri, e2.getMessage()));
            }
        }
    }

}
