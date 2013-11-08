package org.mongodb.operation;


import org.mongodb.CommandResult;
import org.mongodb.MongoCursor;
import org.mongodb.ServerCursor;
import org.mongodb.connection.ServerAddress;

import java.util.Iterator;
import java.util.List;


public class InlineMongoCursor<T> implements MongoCursor<T> {
    private final CommandResult commandResult;
    private final List<T> results;
    private final Iterator<T> iterator;

    public InlineMongoCursor(final CommandResult result, final List<T> results) {
        commandResult = result;
        this.results = results;
        iterator = this.results.iterator();
    }

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
        throw new UnsupportedOperationException("Inline aggregations don't support remove operations.");
    }

    @Override
    public ServerAddress getServerAddress() {
        return commandResult.getAddress();
    }
}
