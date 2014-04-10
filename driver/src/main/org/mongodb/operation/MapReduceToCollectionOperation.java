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

import org.mongodb.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MapReduceStatistics;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.ServerAddress;
import org.mongodb.session.Session;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.CommandDocuments.createMapReduce;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;

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
public class MapReduceToCollectionOperation implements Operation<MapReduceStatistics> {
    private final Document command;
    private final MongoNamespace namespace;
    private final Codec<Document> commandCodec = new DocumentCodec();

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
        this.command = createMapReduce(namespace.getCollectionName(), mapReduce);
    }

    /**
     * Executing this will return a cursor with your results in.
     *
     * @param session
     * @return a MongoCursor that can be iterated over to find all the results of the Map Reduce operation.
     */
    @Override
    public MapReduceStatistics execute(final Session session) {
        CommandResult commandResult = executeWrappedCommandProtocol(namespace.getDatabaseName(), command, commandCodec, commandCodec,
                                                      session);
        serverUsed = commandResult.getAddress();
        return new MapReduceIntoCollectionStatistics(commandResult);
    }

    /**
     * Returns the server that the map reduce operation ran against.
     *
     * @return the server that the results of the map reduce were obtained from
     */
    public ServerAddress getServerUsed() {
        return serverUsed;
    }
}
