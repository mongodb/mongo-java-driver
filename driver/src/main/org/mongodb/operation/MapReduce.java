package org.mongodb.operation;

import org.bson.types.Code;
import org.mongodb.Document;

/**
 * A class that groups arguments for a map/reduce operation.
 */
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

    /**
     * Constructs a new instance of the {@code MapReduce} with output to a another collection.
     *
     * @param mapFunction    a JavaScript function that associates or “maps” a value with a key and emits the key and value pair.
     * @param reduceFunction a JavaScript function that “reduces” to a single object all the values associated with a particular key.
     * @param output         specifies the location of the result of the map-reduce operation.
     */
    public MapReduce(final Code mapFunction, final Code reduceFunction, final MapReduceOutput output) {
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
        this.output = output;
        this.inline = false;
    }

    /**
     * Constructs a new instance of the {@code MapReduce} with result to be inlined into response.
     *
     * @param mapFunction    a JavaScript function that associates or “maps” a value with a key and emits the key and value pair.
     * @param reduceFunction a JavaScript function that “reduces” to a single object all the values associated with a particular key.
     */
    public MapReduce(final Code mapFunction, final Code reduceFunction) {
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
        this.inline = true;

        this.output = null;
    }

    /**
     * Add a 'finalize' argument to the command.
     *
     * @param finalize a JavaScript function that follows the reduce method and modifies the output.
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce finalize(final Code finalize) {
        this.finalizeFunction = finalize;
        return this;
    }

    //CHECKSTYLE:OFF

    /**
     * Add a 'query' argument to the command.
     *
     * @param filter the selection criteria using query operators for determining the documents input to the map function.
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce filter(final Document filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Add a 'sort' argument to the command.
     *
     * @param sortCriteria sorts the input documents. This option is useful for optimization.
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce sort(final Document sortCriteria) {
        this.sortCriteria = sortCriteria;
        return this;
    }

    /**
     * Add a 'scope' argument to the command.
     *
     * @param scope specifies global variables that are accessible in the map , reduce and the finalize functions
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce scope(final Document scope) {
        this.scope = scope;
        return this;
    }

    /**
     * Add a 'limit' argument to the command.
     *
     * @param limit specifies a maximum number of documents to return from the collection.
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce limit(final int limit) {
        this.limit = limit;
        return this;
    }
    //CHECKSTYLE:ON

    /**
     * Add a 'jsMode' flag to the command.
     * <p/>
     * This flag specifies whether to convert intermediate data into BSON format
     * between the execution of the map and reduce functions
     *
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce jsMode() {
        this.jsMode = true;
        return this;
    }

    /**
     * Add a 'verbose' flag to the command.
     * <p/>
     * This flag specifies whether to include the timing information in the result information.
     *
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
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
