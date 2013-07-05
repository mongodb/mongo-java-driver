package org.mongodb.command;

import org.mongodb.operation.CommandResult;


/**
 * A class that represents a result of map/reduce operation.
 *
 */
public class MapReduceInlineCommandResult<T> extends CommandResult {

    /**
     * Constructs a new instance of {@code MapReduceInlineCommandResult} from a {@code CommandResult}
     *
     * @param baseResult result of a command to use as a base
     */
    public MapReduceInlineCommandResult(final CommandResult baseResult) {
        super(baseResult);
    }

    /**
     * Extract the resulting documents of map/reduce operation if is inlined.
     *
     * @return collection of documents to be iterated through
     * @throws IllegalAccessError if resulting documents are not inlined.
     */
    @SuppressWarnings("unchecked")
    public Iterable<T> getResults() {
        return (Iterable<T>) getResponse().get("results");
    }


}
