package uff.ic.swlab.datasetcrawler;

public abstract class Crawler<T> implements AutoCloseable {

    public abstract boolean hasNext();

    public abstract T next();
}
