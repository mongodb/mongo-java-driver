/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Iterator;

class SyncMongoCursor<T> implements MongoCursor<T> {
    private final Iterator<T> iterator;

    SyncMongoCursor(final Publisher<T> publisher, final @Nullable Integer batchSize) {
        iterator = (batchSize == null
                ? Flux.from(publisher).toIterable()
                : Flux.from(publisher).toIterable(batchSize))
                .iterator();
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
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public T tryNext() {
        throw new UnsupportedOperationException();  // No good way to fulfill this contract with a Publisher<T>
    }

    @Override
    public ServerCursor getServerCursor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerAddress getServerAddress() {
        throw new UnsupportedOperationException();
    }

}
