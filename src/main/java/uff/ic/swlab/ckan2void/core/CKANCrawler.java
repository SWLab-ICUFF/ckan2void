package uff.ic.swlab.ckan2void.core;

import eu.trentorise.opendata.jackan.CkanClient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.util.Config;
import uff.ic.swlab.ckan2void.util.Executor;

public class CKANCrawler extends Crawler<Dataset> {

    private Config conf;
    private CkanClient cc;
    private int offset;
    private int limit;
    private List<String> names;
    private Iterator<String> iterator;

    private CKANCrawler() {
    }

    public CKANCrawler(String url) {
        conf = Config.getInsatnce();
        names = new ArrayList<>();
        iterator = names.iterator();
        limit = conf.taskInstances();
        offset = -limit;
        cc = new CkanClient(url);
    }

    @Override
    public Dataset next() {
        Dataset dataset = null;
        while (dataset == null && hasNext())
            dataset = getDataset(iterator.next());
        return dataset;
    }

    public Dataset getDataset(String name) {
        try {
            Callable<Dataset> task = () -> {
                return new Dataset(cc, cc.getDataset(name));
            };
            return Executor.execute(task, "get dataset document \"" + name + "\" from CKAN", conf.taskTimeout());
        } catch (Throwable t) {
            return null;
        }
    }

    private boolean hasNext() {
        try {
            Callable<Boolean> task = () -> {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    offset += limit;
                    names = cc.getDatasetList(limit, offset);
                    iterator = names.iterator();
                    hasNext = iterator.hasNext();
                }
                return hasNext;
            };
            return Executor.execute(task, "asks to " + cc.getCatalogUrl() + " if has a next dataset to retrieve", conf.taskTimeout());
        } catch (Throwable e) {
            names = new ArrayList<>();
            iterator = names.iterator();
            offset = -limit;
            return false;
        }
    }

}
