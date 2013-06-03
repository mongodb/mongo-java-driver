package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.operation.CommandResult;

public class MapReduceCommandResult<T> extends CommandResult {

    public MapReduceCommandResult(final CommandResult baseResult) {
        super(baseResult);
    }

    public boolean isInline() {
        return getResponse().containsKey("results");
    }

    @SuppressWarnings("unchecked")
    public Iterable<T> getValue() {
        return (Iterable<T>) getResponse().get("results");
    }

    public String getTargetCollectionName() {
        final Object target = getTarget();
        if (target instanceof String) {
            return (String) target;
        } else {
            return ((Document) target).getString("collection");
        }
    }

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
            throw new IllegalAccessError("Map-reduce is done with 'inline' flag");
        }
        return getResponse().get("result");
    }
}
