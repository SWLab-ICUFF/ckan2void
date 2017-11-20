package uff.ic.swlab.ckan2void.core;

import eu.trentorise.opendata.jackan.CkanClient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import uff.ic.swlab.ckan2void.util.Dataset;
import uff.ic.swlab.ckan2void.util.Config;
import uff.ic.swlab.ckan2void.util.Executor;

public class CKANCrawler extends Crawler<Dataset> {

    private String url;
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
        cc = new CkanClient(url);
        limit = conf.taskInstances();
        offset = -limit;
        names = new ArrayList<>();
        iterator = names.iterator();
        this.url = url;
    }

    @Override
    public synchronized Dataset next() {
        Dataset dataset = null;
        String name = null;

        while ((name == null || name.equals("")) && hasNext())
            name = iterator.next();

        if (name != null && !name.equals(""))
            dataset = new Dataset(cc, name);

        return dataset;
    }

    private boolean hasNext() {
        try {
            Callable<Boolean> task = () -> {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    cc = new CkanClient(url);
                    offset += limit;
                    names = cc.getDatasetList(limit, offset);
                    iterator = names.iterator();
                    hasNext = iterator.hasNext();
                }
                return hasNext;
            };
            return Executor.execute(task, "Ask to " + cc.getCatalogUrl() + " if it has a next dataset to retrieve", conf.httpAccessTimeout());
        } catch (Throwable e) {
            names = new ArrayList<>();
            iterator = names.iterator();
            //offset = -limit;
            return false;
        }
    }

}
