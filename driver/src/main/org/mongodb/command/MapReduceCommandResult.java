package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.operation.CommandResult;

/**
 * A class that represents a result of map/reduce operation.
 */
public class MapReduceCommandResult extends CommandResult {

    /**
     * Constructs a new instance of {@code MapReduceCommandResult} from a {@code CommandResult}
     *
     * @param baseResult result of a command to use as a base
     */
    public MapReduceCommandResult(final CommandResult baseResult) {
        super(baseResult);
    }

    /**
     * Get a name of the collection that was used by map/reduce operation to write its output.
     *
     * @return a collection name
     */
    public String getCollectionName() {
        final Object result = getResponse().get("result");
        return (result instanceof Document)
                ? ((Document) result).getString("collection")
                : (String) result;
    }

    /**
     * Get a name of the database that was used by map/reduce operation to write its output.
     *
     * @return a database name
     */
    public String getDatabaseName() {
        final Object result = getResponse().get("result");
        return (result instanceof Document)
                ? ((Document) result).getString("db")
                : null;
    }
}
