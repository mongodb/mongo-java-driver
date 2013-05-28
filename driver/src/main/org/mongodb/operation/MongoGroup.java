package org.mongodb.operation;

import org.mongodb.Document;

public class MongoGroup {

    private final Document key;
    private final String keyf;
    private final String reduce;
    private final Document initial;
    private Document cond;
    private String finalize;

    private MongoGroup(final Document key, final String keyf, final String reduce, final Document initial) {
        this.keyf = keyf;
        this.key = key;
        this.reduce = reduce;
        this.initial = initial;
    }

    public MongoGroup(final Document key, final String reduce, final Document initial) {
        this(key, null, reduce, initial);
    }

    public MongoGroup(final String keyf, final String reduce, final Document initial) {
        this(null, keyf, reduce, initial);
    }

    public MongoGroup cond(final Document aCond) {
        this.cond = aCond;
        return this;
    }

    public MongoGroup finalize(final String finalizeFunction) {
        this.finalize = finalizeFunction;
        return this;
    }

    public Document getKey() {
        return key;
    }

    public Document getCond() {
        return cond;
    }

    public Document getInitial() {
        return initial;
    }

    public String getReduce() {
        return reduce;
    }

    public String getFinalize() {
        return finalize;
    }

    public String getKeyf() {
        return keyf;
    }
}
