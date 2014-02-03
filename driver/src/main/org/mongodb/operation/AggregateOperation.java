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

import org.mongodb.AggregationCursor;
import org.mongodb.AggregationOptions;
import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ServerAddress;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class AggregateOperation<T> extends BaseOperation<MongoCursor<T>> {
    private static final Logger LOGGER = Loggers.getLogger("operation");

    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private final DocumentCodec commandEncoder = new DocumentCodec(PrimitiveCodecs.createDefault());
    private final List<Document> pipeline;
    private final AggregationOptions options;
    private final ServerAddress serverAddress;
    private final Document command;
    private final ServerConnectionProvider connectionProvider;

    public AggregateOperation(final MongoNamespace namespace, final List<Document> pipeline, final Decoder<T> decoder,
                              final AggregationOptions options, final BufferProvider bufferProvider, final Session session,
                              final boolean closeSession, final ReadPreference readPreference) {
        super(bufferProvider, session, closeSession);

        this.namespace = namespace;
        this.decoder = decoder;
        this.pipeline = pipeline;
        this.options = options;
        command = asCommandDocument();
        connectionProvider = getConnectionProvider(readPreference);
        serverAddress = connectionProvider.getServerDescription().getAddress();
    }

    @SuppressWarnings("unchecked")
    @Override
    public MongoCursor<T> execute() {
        CommandResult result = sendMessage();
        if (options.getOutputMode() == AggregationOptions.OutputMode.INLINE) {
            return new InlineMongoCursor<T>(result, (List<T>) result.getResponse()
                                                                    .get("result"));
        } else {
            return new AggregationCursor<T>(options, namespace, decoder, getBufferProvider(), getSession(), isCloseSession(),
                                            connectionProvider, receiveMessage(result));
        }
    }

    public CommandResult explain() {
        command.put("explain", true);
        return sendMessage();
    }

    private CommandResult sendMessage() {
        if (options.getOutputMode() == AggregationOptions.OutputMode.CURSOR) {
            Document cursor = new Document();
            if (options.getBatchSize() != null) {
                cursor.put("batchSize", options.getBatchSize());
            }
            command.put("cursor", cursor);
        }
        if (options.getAllowDiskUse() != null) {
            command.put("allowDiskUse", options.getAllowDiskUse());
        }

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest(command.toString());
        }
        return new CommandProtocol(namespace.getDatabaseName(),
                                   command,
                                   commandEncoder,
                                   new CommandResultWithPayloadDecoder<T>(decoder),
                                   getBufferProvider(),
                                   connectionProvider.getServerDescription(),
                                   connectionProvider.getConnection(),
                                   false).execute();
    }

    private QueryResult<T> receiveMessage(final CommandResult result) {
        if (result.isOk()) {
            return new QueryResult<T>(result, serverAddress);
        } else {
            throw new MongoCommandFailureException(result);
        }
    }

    @Override
    public String toString() {
        return String.format("AggregateOperation{namespace=%s, pipeline=%s, options=%s}", namespace, pipeline, options);
    }

    public Document getCommand() {
        return command;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    private Document asCommandDocument() {
        Document commandDocument = new Document("aggregate", namespace.getCollectionName());
        commandDocument.put("pipeline", pipeline);
        if (options.getMaxTime(MILLISECONDS) > 0) {
            commandDocument.put("maxTimeMS", options.getMaxTime(MILLISECONDS));
        }
        return commandDocument;
    }
}
