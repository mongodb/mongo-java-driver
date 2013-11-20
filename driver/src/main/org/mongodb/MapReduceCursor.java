package org.mongodb;

import org.mongodb.connection.ServerAddress;
import org.mongodb.operation.SingleResultFuture;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Cursor representation of the results of an inline map-reduce operation.  This allows users to iterate over the results that were returned
 * from the operation, and also provides access to the statistics returned in the results.
 *
 * @param <T> the type of document to return in the results.
 * @since 3.0
 */
public class MapReduceCursor<T> implements MongoCursor<T>, MapReduceStatistics, MongoIterable<T> {
    private final CommandResult commandResult;
    private final List<T> results;
    private final Iterator<T> iterator;

    /**
     * Create a new cursor from the CommandResult which contains the result of running a map reduce
     *
     * @param result a CommandResult containing the results of running an inline map-reduce
     */
    @SuppressWarnings("unchecked")
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

    @Override
    public MongoCursor<T> iterator() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void forEach(final Block<? super T> block) {
        for (final T result : results) {
            if (!block.run(result)) {
                break;
            }
        }
    }

    @Override
    public <A extends Collection<? super T>> A into(final A target) {
        forEach(new Block<T>() {
            @Override
            public boolean run(final T t) {
                target.add(t);
                return true;
            }
        });
        return target;
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    @Override
    public void asyncForEach(final AsyncBlock<? super T> block) {
        forEach(block);
    }

    @Override
    public <A extends Collection<? super T>> MongoFuture<A> asyncInto(final A target) {
        return new SingleResultFuture<A>(into(target), null);
    }
}
