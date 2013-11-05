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

package org.mongodb.operation;

import org.mongodb.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

/**
 * Operation encapsulating everything for running a Map Reduce against a MongoDB instance.
 *
 * @see <a href="http://docs.mongodb.org/manual/core/map-reduce/">Map-Reduce</a>
 */
public class MapReduceOperation extends BaseOperation<CommandResult> {
    private final Document command;
    private final MongoNamespace namespace;
    private final ReadPreference readPreference;
    private final DocumentCodec resultDecoder;
    private final Codec<Document> commandCodec = new DocumentCodec();

    /**
     * Construct a MapReduceOperation with all the criteria it needs to execute
     *
     * @param namespace      the database and collection to perform the map reduce on
     * @param mapReduce      the bean containing all the details of the Map Reduce operation to perform
     * @param resultDecoder  the decoder to use to decode the CommandResult containing the results
     * @param readPreference the read preference suggesting which server to run the command on
     * @param bufferProvider the BufferProvider to use when reading or writing to the network
     * @param session        the current Session, which will give access to a connection to the MongoDB instance
     * @param closeSession   true if the session should be closed at the end of the execute method
     */
    public MapReduceOperation(final MongoNamespace namespace, final MapReduce mapReduce, final DocumentCodec resultDecoder,
                              final ReadPreference readPreference, final BufferProvider bufferProvider,
                              final Session session,
                              final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.namespace = namespace;
        this.readPreference = readPreference;
        this.resultDecoder = resultDecoder;
        this.command = createCommandDocument(namespace.getCollectionName(), mapReduce);
    }

    /**
     * Executing this will return a command result.  If all went well, this contains the results of the map reduce.
     *
     * @return CommandResult which will contain either the results of the map reduce, or an error message stating what went wrong
     */
    @Override
    public CommandResult execute() {
        ServerConnectionProviderOptions options = getServerConnectionProviderOptions();
        ServerConnectionProvider provider = getSession().createServerConnectionProvider(options);
        return new CommandProtocol(namespace.getDatabaseName(), command, commandCodec, resultDecoder, getBufferProvider(),
                                   provider.getServerDescription(), provider.getConnection(), true)
                   .execute();
    }

    private ServerConnectionProviderOptions getServerConnectionProviderOptions() {
        return new ServerConnectionProviderOptions(true, new ReadPreferenceServerSelector(readPreference));
    }

    /*
     * Package protected so that it can be tested.  Not my favourite solution but the best way to test given the current architecture.
     */
    static Document createCommandDocument(final String collectionName, final MapReduce mapReduce) {

        return new Document("mapReduce", collectionName).append("map", mapReduce.getMapFunction())
                                                        .append("reduce", mapReduce.getReduceFunction())
                                                        .append("out", mapReduce.isInline() ? new Document("inline", 1)
                                                                                            : outputAsDocument(mapReduce.getOutput()))
                                                        .append("query", mapReduce.getFilter())
                                                        .append("sort", mapReduce.getSortCriteria())
                                                        .append("limit", mapReduce.getLimit())
                                                        .append("finalize", mapReduce.getFinalizeFunction())
                                                        .append("scope", mapReduce.getScope())
                                                        .append("jsMode", mapReduce.isJsMode())
                                                        .append("verbose", mapReduce.isVerbose());
    }

    private static Document outputAsDocument(final MapReduceOutput output) {
        Document document = new Document(output.getAction().getValue(), output.getCollectionName());
        if (output.getDatabaseName() != null) {
            document.append("db", output.getDatabaseName());
        }
        document.append("sharded", output.isSharded());
        document.append("nonAtomic", output.isNonAtomic());

        return document;
    }

}
