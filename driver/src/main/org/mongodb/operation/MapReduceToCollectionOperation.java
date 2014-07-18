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

import com.mongodb.ServerAddress;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import org.bson.BsonDocument;
import org.mongodb.CommandResult;
import org.mongodb.Function;
import org.mongodb.MapReduceStatistics;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;

import static com.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.CommandDocuments.createMapReduce;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;

/**
 * Operation that runs a Map Reduce against a MongoDB instance.  This operation does not support "inline" results, i.e. the results will be
 * output into the collection represented by the MongoNamespace provided.
 * <p/>
 * To run a map reduce operation and receive the results inline (i.e. as a response to running the command) use {@code
 * MapReduceWithInlineResultsOperation}.
 *
 * @mongodb.driver.manual core/map-reduce Map-Reduce
 * @since 3.0
 */
public class MapReduceToCollectionOperation implements AsyncWriteOperation<MapReduceStatistics>, WriteOperation<MapReduceStatistics> {
    private final MapReduce mapReduce;
    private final MongoNamespace namespace;
    private ServerAddress serverUsed;

    /**
     * Construct a MapReduceOperation with all the criteria it needs to execute
     *
     * @param namespace the database and collection to perform the map reduce on
     * @param mapReduce the bean containing all the details of the Map Reduce operation to perform
     */
    public MapReduceToCollectionOperation(final MongoNamespace namespace, final MapReduce mapReduce) {
        if (mapReduce.isInline()) {
            throw new IllegalArgumentException("This operation can only be used with map reduce operations that put the results into a "
                                               + "collection.  Invalid MapReduce: " + mapReduce);
        }
        this.namespace = notNull("namespace", namespace);
        this.mapReduce = mapReduce;
    }

    /**
     * Executing this will return a cursor with your results in.
     *
     * @param binding the binding
     * @return a MongoCursor that can be iterated over to find all the results of the Map Reduce operation.
     */
    @Override
    public MapReduceStatistics execute(final WriteBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), binding, transformer());
    }

    @Override
    public MongoFuture<MapReduceStatistics> executeAsync(final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), binding, transformer());
    }

    /**
     * Returns the server that the map reduce operation ran against.
     *
     * @return the server that the results of the map reduce were obtained from
     */
    public ServerAddress getServerUsed() {
        return serverUsed;
    }

    private Function<CommandResult, MapReduceStatistics> transformer() {
        return new Function<CommandResult, MapReduceStatistics>() {
            @SuppressWarnings("unchecked")
            @Override
            public MapReduceStatistics apply(final CommandResult result) {
                serverUsed = result.getAddress();
                return MapReduceHelper.createStatistics(result);
            }
        };
    }

    private BsonDocument getCommand() {
        return createMapReduce(namespace.getCollectionName(), mapReduce);
    }
}
