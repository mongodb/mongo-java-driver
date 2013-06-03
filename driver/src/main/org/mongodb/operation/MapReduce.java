package org.mongodb.operation;

import org.bson.types.Code;
import org.mongodb.Document;

public class MapReduce {
    private final Code mapFunction;
    private final Code reduceFunction;
    private final MapReduceOutput output;
    private final boolean inline;
    private Code finalizeFunction;
    private Document scope;
    private Document filter;
    private Document sortCriteria;
    private int limit;
    private boolean jsMode;
    private boolean verbose;

    public MapReduce(final Code mapFunction, final Code reduceFunction, final MapReduceOutput output) {
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
        this.output = output;
        this.inline = false;
    }

    public MapReduce(final Code mapFunction, final Code reduceFunction) {
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
        this.inline = true;

        this.output = null;
    }

    public MapReduce finalize(final Code finalize) {
        this.finalizeFunction = finalize;
        return this;
    }

    //CHECKSTYLE:OFF TODO: http://checkstyle.sourceforge.net/config_coding.html#HiddenField needs to be supressed?
    public MapReduce filter(final Document filter) {
        this.filter = filter;
        return this;
    }

    public MapReduce sort(final Document sortCriteria) {
        this.sortCriteria = sortCriteria;
        return this;
    }

    public MapReduce scope(final Document scope) {
        this.scope = scope;
        return this;
    }

    public MapReduce limit(final int limit) {
        this.limit = limit;
        return this;
    }
    //CHECKSTYLE:ON

    public MapReduce jsMode() {
        this.jsMode = true;
        return this;
    }

    public MapReduce verbose() {
        this.verbose = true;
        return this;
    }


    public Code getMapFunction() {
        return mapFunction;
    }

    public Code getReduceFunction() {
        return reduceFunction;
    }

    public Code getFinalizeFunction() {
        return finalizeFunction;
    }

    public Document getFilter() {
        return filter;
    }

    public Document getSortCriteria() {
        return sortCriteria;
    }

    public Document getScope() {
        return scope;
    }

    public MapReduceOutput getOutput() {
        return output;
    }

    public int getLimit() {
        return limit;
    }

    public boolean isJsMode() {
        return jsMode;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public boolean isInline() {
        return inline;
    }
}
