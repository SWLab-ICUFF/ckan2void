package uff.ic.swlab.ckan2void.core;

import eu.trentorise.opendata.jackan.CkanClient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import uff.ic.swlab.ckan2void.adapter.Dataset;

public class CKANCrawler extends Crawler<Dataset> {

    private CkanClient cc = null;
    private int offset = 0;
    private int limit = 2000;
    private List<String> names;
    private Iterator<String> iterator;

    private CKANCrawler() {
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
    public boolean hasNext() {
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

    @Override
    public Dataset next() {
        return new Dataset(cc, cc.getDataset(iterator.next()));
    }

    public Dataset getDataset(String name) {
        return new Dataset(cc, cc.getDataset(name));
    }

    @Override
    public void close() {
    }
}
