/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb;

import org.mongodb.async.AsyncBlock;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;

import java.util.Collection;

class MappingIterable<U, V> implements MongoIterable<V> {

    private final MongoIterable<U> iterable;
    private final Function<U, V> mapper;

    public MappingIterable(final MongoIterable<U> iterable, final Function<U, V> mapper) {
        this.iterable = iterable;
        this.mapper = mapper;
    }

    @Override
    public MongoCursor<V> iterator() {
        return new MongoMappingCursor<U, V>(iterable.iterator(), mapper);
    }

    @Override
    public void forEach(final Block<? super V> block) {
        iterable.forEach(new Block<U>() {
            @Override
            public boolean run(final U document) {
                return block.run(mapper.apply(document));
            }
        });
    }

    @Override
    public <A extends Collection<? super V>> A into(final A target) {
        forEach(new Block<V>() {
            @Override
            public boolean run(final V v) {
                target.add(v);
                return true;
            }
        });
        return target;
    }

    @Override
    public <W> MongoIterable<W> map(final Function<V, W> newMap) {
        return new MappingIterable<V, W>(this, newMap);
    }

    @Override
    public void asyncForEach(final AsyncBlock<? super V> block) {
        iterable.asyncForEach(new AsyncBlock<U>() {
            @Override
            public void done() {
                block.done();
            }

            @Override
            public boolean run(final U u) {
                return block.run(mapper.apply(u));
            }
        });
    }

    @Override
    public <A extends Collection<? super V>> MongoFuture<A> asyncInto(final A target) {
        final SingleResultFuture<A> future = new SingleResultFuture<A>();
        asyncInto(target, new SingleResultFutureCallback<A>(future));
        return future;
    }

    private <A extends Collection<? super V>> void asyncInto(final A target, final SingleResultCallback<A> callback) {
        asyncForEach(new AsyncBlock<V>() {
            @Override
            public void done() {
                callback.onResult(target, null);
            }

            @Override
            public boolean run(final V v) {
                target.add(v);
                return true;
            }
        });
    }
}
