/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import org.mongodb.MapReduceAsyncCursor;
import org.mongodb.connection.ServerAddress;

import java.util.List;

/**
 * Cursor representation of the results of an inline map-reduce operation.  This allows users to iterate over the results that were returned
 * from the operation, and also provides access to the statistics returned in the results.
 *
 * @param <T> the type of document to return in the results.
 * @since 3.0
 */
class MapReduceInlineResultsAsyncCursor<T> extends InlineMongoAsyncCursor<T> implements MapReduceAsyncCursor<T> {
    private final CommandResult commandResult;

    @SuppressWarnings("unchecked")
    MapReduceInlineResultsAsyncCursor(final CommandResult result) {
        super((List<T>) result.getResponse().get("results"));
        commandResult = result;
    }

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
