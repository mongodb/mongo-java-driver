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
import org.mongodb.operation.CommandResult;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.ServerCursor;
import org.mongodb.operation.SingleResultFuture;

import java.util.Collection;
import java.util.Iterator;

class MappedReducedIterable<T> implements MongoIterable<T> {
    private CommandResult commandResult;

    public MappedReducedIterable(final CommandResult commandResult) {
        this.commandResult = commandResult;
    }

    @Override
    public MongoCursor<T> iterator() {
        return new MappedReducedCursor();
    }

    @Override
    public void forEach(final Block<? super T> block) {
        for (T document : getResults()) {
            if (!block.run(document)) {
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
        for (T document : getResults()) {
            if (!block.run(document)) {
                break;
            }
        }
        block.done();
    }

    @Override
    public <A extends Collection<? super T>> MongoFuture<A> asyncInto(final A target) {
        return new SingleResultFuture<A>(into(target), null);
    }

    @SuppressWarnings("unchecked")
    private Iterable<T> getResults() {
        return ((Iterable<T>) commandResult.getResponse().get("results"));
    }

    private class MappedReducedCursor implements MongoCursor<T> {
        private final Iterator<T> iterator = getResults().iterator();
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
            throw new UnsupportedOperationException();
        }
    }
}
