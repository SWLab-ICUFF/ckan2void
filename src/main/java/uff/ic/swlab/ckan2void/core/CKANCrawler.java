package uff.ic.swlab.ckan2void.core;

import eu.trentorise.opendata.jackan.CkanClient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import uff.ic.swlab.ckan2void.adapter.Dataset;
import uff.ic.swlab.ckan2void.util.Config;

public class CKANCrawler extends Crawler<Dataset> {

    private Config conf;
    private CkanClient cc = null;
    private int offset = 0;
    private int limit;
    private List<String> names;
    private Iterator<String> iterator;

    private CKANCrawler() {
        conf = Config.getInsatnce();
        limit = conf.taskInstances();
    }

    public CKANCrawler(String url) {
        try {
            cc = new CkanClient(url);
            names = cc.getDatasetList(limit, offset);
            iterator = names.iterator();
        } catch (Throwable e) {
            System.out.println("CKAN error while connecting to CKAN!");
            names = new ArrayList<>();
            iterator = names.iterator();
        }
    }

    @Override
    public Dataset next() {
        while (hasNext())
            try {
                return getDataset(iterator.next());
            } catch (Throwable e) {
            }
        return null;
    }

    public Dataset getDataset(String name) {
        try {
            return new Dataset(cc, cc.getDataset(name));
        } catch (Throwable t) {
            return null;
        }
    }

    private boolean hasNext() {
        try {
            boolean hasNext = iterator.hasNext();
            if (!hasNext) {
                offset += limit;
                names = cc.getDatasetList(limit, offset);
                iterator = names.iterator();
                hasNext = iterator.hasNext();
            }
            return hasNext;
        } catch (Throwable e) {
            return false;
        }
    }

}
