package uff.ic.swlab.ckan2void.core;

public abstract class Crawler<T> implements AutoCloseable {

    public abstract boolean hasNext();

    public abstract T next();
}
