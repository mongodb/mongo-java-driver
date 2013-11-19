package org.mongodb.operation;

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MapReduceStatistics;
import org.mongodb.MongoCursor;
import org.mongodb.ServerCursor;
import org.mongodb.connection.ServerAddress;

import java.util.Iterator;
import java.util.List;

/**
 * Cursor representation of the results of an inline map-reduce operation.  This allows users to iterate over the results that were returned
 * from the operation, and also provides access to the statistics returned in the results.
 *
 * @param <T>
 * @since 3.0
 */
public class MapReduceCursor<T> implements MongoCursor<T>, MapReduceStatistics {
    private final CommandResult commandResult;
    private final List<T> results;
    private final Iterator<T> iterator;

    /**
     * Create a new cursor from the CommandResult which contains the result of running a map reduce
     *
     * @param result a CommandResult containing the results of running an inline map-reduce
     */
    public MapReduceCursor(final CommandResult result) {
        commandResult = result;
        this.results = (List<T>) commandResult.getResponse().get("results");
        iterator = this.results.iterator();
    }

    /**
     * Returns the original format of the map-reduce results.  Not recommended for public use, getter methods on this cursor should provide
     * access to all the information that is required.
     *
     * @return the CommandResult returned by the server
     */
    public CommandResult getCommandResult() {
        return commandResult;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }

    @Override
    public ServerCursor getServerCursor() {
        return null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Inline map reduce operations don't support remove operations.");
    }

    @Override
    public ServerAddress getServerAddress() {
        return commandResult.getAddress();
    }

    @Override
    public int getInputCount() {
        return ((Document) commandResult.getResponse().get("counts")).getInteger("input");
    }

    @Override
    public int getOutputCount() {
        return ((Document) commandResult.getResponse().get("counts")).getInteger("output");
    }

    @Override
    public int getEmitCount() {
        return ((Document) commandResult.getResponse().get("counts")).getInteger("emit");
    }

    @Override
    public int getDuration() {
        return commandResult.getResponse().getInteger("timeMillis");
    }
}
