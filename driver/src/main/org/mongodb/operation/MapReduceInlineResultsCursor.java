/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.mongodb.operation;

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MapReduceCursor;
import org.mongodb.ServerCursor;
import org.mongodb.connection.ServerAddress;

import java.util.Iterator;
import java.util.List;

/**
 * Cursor representation of the results of an inline map-reduce operation.  This allows users to iterate over the results that were returned
 * from the operation, and also provides access to the statistics returned in the results.
 *
 * @param <T> the type of document to return in the results.
 * @since 3.0
 */
class MapReduceInlineResultsCursor<T> implements MapReduceCursor<T> {
    private final CommandResult commandResult;
    private final List<T> results;
    private final Iterator<T> iterator;

    @SuppressWarnings("unchecked")
    MapReduceInlineResultsCursor(final CommandResult result) {
        commandResult = result;
        this.results = (List<T>) commandResult.getResponse().get("results");
        iterator = this.results.iterator();
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
