package org.mongodb.async;

import org.mongodb.Block;
import org.mongodb.Function;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.SingleResultFuture;

import java.util.Collection;

class MappingIterable<T, U> implements MongoIterable<U> {
    private final MongoIterable<T> iterable;
    private final Function<T, U> mapper;

    public MappingIterable(final MongoIterable<T> iterable, final Function<T, U> mapper) {
        this.iterable = iterable;
        this.mapper = mapper;
    }

    @Override
    public MongoFuture<Void> forEach(final Block<? super U> block) {
        final SingleResultFuture<Void> future = new SingleResultFuture<Void>();
        iterable.forEach(new Block<T>() {
            @Override
            public void apply(final T t) {
                block.apply(mapper.apply(t));
            }
        }).register(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    future.init(null, null);
                }
            }
        });
        return future;
    }

    @Override
    public <A extends Collection<? super U>> MongoFuture<A> into(final A target) {
        final SingleResultFuture<A> future = new SingleResultFuture<A>();
        iterable.forEach(new Block<T>() {
            @Override
            public void apply(final T t) {
                target.add(mapper.apply(t));
            }
        }).register(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    future.init(target, null);
                }
            }
        });
        return future;
    }

    @Override
    public <V> MongoIterable<V> map(final Function<U, V> mapper) {
        return new MappingIterable<U, V>(this, mapper);
    }
}
