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
import org.mongodb.Function;
import org.mongodb.MongoFuture;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.ReadBinding;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;

/**
 * Execute this operation to return a List of Strings of the names of all the databases for the current MongoDB instance.
 */
public class GetDatabaseNamesOperation implements AsyncReadOperation<List<String>>, ReadOperation<List<String>> {

    /**
     * Executing this will return a list of all the databases names in the MongoDB instance.
     *
     * @return a List of Strings of the names of all the databases in the MongoDB instance
     * @param binding the binding
     */
    @Override
    public List<String> execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol("admin", getCommand(), binding, transformer());
    }

    /**
     * Executing this will return a Future list of all the databases names in the MongoDB instance.
     *
     * @return a Future List of Strings of the names of all the databases in the MongoDB instance
     * @param binding the binding
     */
    @Override
    public MongoFuture<List<String>> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync("admin", getCommand(), binding, transformer());
    }

    private Function<CommandResult, List<String>> transformer() {
        return new Function<CommandResult, List<String>>() {
            @SuppressWarnings("unchecked")
            @Override
            public List<String> apply(final CommandResult result) {
                List<Document> databases = (List<Document>) result.getResponse().get("databases");

                List<String> databaseNames = new ArrayList<String>();
                for (final Document database : databases) {
                    databaseNames.add(database.get("name", String.class));
                }
                return unmodifiableList(databaseNames);
            }
        };
    }

    private Document getCommand() {
        return new Document("listDatabases", 1);
    }
}
