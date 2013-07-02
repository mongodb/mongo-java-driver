package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.operation.CommandResult;

/**
 * A class that represents a result of map/reduce operation.
 */
public class MapReduceCommandResult<T> extends CommandResult {

    /**
     * Constructs a new instance of {@code MapReduceCommandResult} from a {@code CommandResult}
     *
     * @param baseResult result of a command to use as a base
     */
    public MapReduceCommandResult(final CommandResult baseResult) {
        super(baseResult);
    }

    /**
     * Check if the resulting documents of map/reduce operation is inlined.
     *
     * @return true if map/reduce operation performed with {'inline':1} argument, false otherwise.
     */
    public boolean isInline() {
        return getResponse().containsKey("results");
    }

    /**
     * Extract the resulting documents of map/reduce operation if is inlined.
     *
     * @return collection of documents to be iterated through
     * @throws IllegalAccessError if resulting documents are not inlined.
     */
    @SuppressWarnings("unchecked")
    public Iterable<T> getValue() {
        if (isInline()) {
            return (Iterable<T>) getResponse().get("results");
        } else {
            throw new IllegalAccessError("Map/reduce operation is done without 'inline' flag");
        }
    }

    /**
     * Get a name of the collection that was used by map/reduce operation to write its output.
     *
     * @return a collection name
     * @throws IllegalAccessError if resulting documents are inlined.
     */
    public String getTargetCollectionName() {
        final Object target = getTarget();
        if (target instanceof String) {
            return (String) target;
        } else {
            return ((Document) target).getString("collection");
        }
    }

    /**
     * Get a name of the database that was used by map/reduce operation to write its output.
     *
     * @return a database name
     * @throws IllegalAccessError if resulting documents are inlined.
     */
    public String getTargetDatabaseName() {
        final Object target = getTarget();
        if (target instanceof String) {
            return null;
        } else {
            return ((Document) target).getString("db");
        }
    }

    private Object getTarget() {
        if (isInline()) {
            throw new IllegalAccessError("Map/reduce operation is done with 'inline' flag");
        }
        return getResponse().get("result");
    }
}
