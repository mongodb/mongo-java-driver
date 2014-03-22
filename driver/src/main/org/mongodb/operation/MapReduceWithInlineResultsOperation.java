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
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MapReduceCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.CommandDocuments.createMapReduce;

/**
 * Operation that runs a Map Reduce against a MongoDB instance.  This operation only supports "inline" results, i.e. the results will be
 * returned as a result of running this operation.
 * <p/>
 * To run a map reduce operation into a given collection, use {@code MapReduceToCollectionOperation}.
 *
 * @param <T> the type contained in the collection
 * @mongodb.driver.manual core/map-reduce Map-Reduce
 * @since 3.0
 */
public class MapReduceWithInlineResultsOperation<T> implements Operation<MapReduceCursor<T>> {
    private final Document command;
    private final MongoNamespace namespace;
    private final ReadPreference readPreference;
    private final MapReduceCommandResultCodec<T> mapReduceResultDecoder;
    private final Codec<Document> commandCodec = new DocumentCodec();

    /**
     * Construct a MapReduceOperation with all the criteria it needs to execute
     * @param namespace      the database and collection to perform the map reduce on
     * @param mapReduce      the bean containing all the details of the Map Reduce operation to perform
     * @param decoder        the decoder to use for decoding the documents in the results of the map-reduce operation
     * @param readPreference the read preference suggesting which server to run the command on
     */
    public MapReduceWithInlineResultsOperation(final MongoNamespace namespace, final MapReduce mapReduce,
                                               final Decoder<T> decoder, final ReadPreference readPreference) {
        super();
        if (!mapReduce.isInline()) {
            throw new IllegalArgumentException("This operation can only be used with inline map reduce operations.  Invalid MapReduce: "
                                               + mapReduce);
        }
        this.namespace = notNull("namespace", namespace);
        this.readPreference = readPreference;
        this.mapReduceResultDecoder = new MapReduceCommandResultCodec<T>(PrimitiveCodecs.createDefault(), decoder);
        this.command = createMapReduce(namespace.getCollectionName(), mapReduce);
    }

    /**
     * Executing this will return a cursor with your results and the statistics in.
     *
     * @return a MapReduceCursor that can be iterated over to find all the results of the Map Reduce operation.
     * @param session
     */
    @Override
    @SuppressWarnings("unchecked")
    public MapReduceCursor<T> execute(final Session session) {
        ServerConnectionProvider provider = session.createServerConnectionProvider(getServerConnectionProviderOptions());
        CommandResult commandResult = new CommandProtocol(namespace.getDatabaseName(), command, commandCodec, mapReduceResultDecoder,
                                                          provider.getServerDescription(), provider.getConnection(),
                                                          true)
                                          .execute();

        return new MapReduceInlineResultsCursor<T>(commandResult);
    }

    private ServerConnectionProviderOptions getServerConnectionProviderOptions() {
        return new ServerConnectionProviderOptions(true, new ReadPreferenceServerSelector(readPreference));
    }
}
