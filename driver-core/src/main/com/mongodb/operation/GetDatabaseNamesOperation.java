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

package com.mongodb.operation;

import com.mongodb.Function;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;

/**
 * Execute this operation to return a List of Strings of the names of all the databases for the current MongoDB instance.
 */
public class GetDatabaseNamesOperation implements AsyncReadOperation<List<String>>, ReadOperation<List<String>> {

    /**
     * Executing this will return a list of all the databases names in the MongoDB instance.
     *
     * @param binding the binding.
     * @return a List of Strings of the names of all the databases in the MongoDB instance.
     */
    @Override
    public List<String> execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol("admin", getCommand(), binding, transformer());
    }

    /**
     * Executing this will return a Future list of all the databases names in the MongoDB instance.
     *
     * @param binding the binding
     * @return a Future List of Strings of the names of all the databases in the MongoDB instance
     */
    @Override
    public MongoFuture<List<String>> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync("admin", getCommand(), binding, transformer());
    }

    private Function<BsonDocument, List<String>> transformer() {
        return new Function<BsonDocument, List<String>>() {
            @SuppressWarnings("unchecked")
            @Override
            public List<String> apply(final BsonDocument result) {
                BsonArray databases = result.getArray("databases");

                List<String> databaseNames = new ArrayList<String>();
                for (final BsonValue database : databases) {
                    databaseNames.add(database.asDocument().getString("name").getValue());
                }
                return databaseNames;
            }
        };
    }

    private BsonDocument getCommand() {
        return new BsonDocument("listDatabases", new BsonInt32(1));
    }
}
