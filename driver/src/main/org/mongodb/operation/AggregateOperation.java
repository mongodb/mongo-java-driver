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
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class AggregateOperation<T> implements Operation<MongoCursor<T>> {
    private static final Logger LOGGER = Loggers.getLogger("operation");

    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private final DocumentCodec commandEncoder = new DocumentCodec(PrimitiveCodecs.createDefault());
    private final List<Document> pipeline;
    private final AggregationOptions options;
    private final ReadPreference readPreference;
    private final Document command;

    public AggregateOperation(final MongoNamespace namespace, final List<Document> pipeline, final Decoder<T> decoder,
                              final AggregationOptions options, final ReadPreference readPreference) {
        this.namespace = namespace;
        this.decoder = decoder;
        this.pipeline = pipeline;
        this.options = options;
        this.readPreference = readPreference;
        command = asCommandDocument();
    }

    @SuppressWarnings("unchecked")
    @Override
    public MongoCursor<T> execute(final Session session) {
        CommandResult result = sendMessage(session);
        if (options.getOutputMode() == AggregationOptions.OutputMode.INLINE) {
            return new InlineMongoCursor<T>(result, (List<T>) result.getResponse().get("result"));
        } else {
            return new AggregationCursor<T>(options, namespace, decoder, OperationHelper.getConnectionProvider(readPreference, session),
                                            receiveMessage(result));
        }
    }

    public CommandResult explain(final Session session) {
        command.put("explain", true);
        return sendMessage(session);
    }

    private CommandResult sendMessage(final Session session) {
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

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(command.toString());
        }

        ServerConnectionProvider provider = OperationHelper.getConnectionProvider(readPreference, session);

        return new CommandProtocol(namespace.getDatabaseName(),
                                   command,
                                   commandEncoder,
                                   new CommandResultWithPayloadDecoder<T>(decoder),
                                   provider.getServerDescription(),
                                   provider.getConnection(),
                                   true).execute();
    }

    private QueryResult<T> receiveMessage(final CommandResult result) {
        if (result.isOk()) {
            return new QueryResult<T>(result, result.getAddress());
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

    private Document asCommandDocument() {
        Document commandDocument = new Document("aggregate", namespace.getCollectionName());
        commandDocument.put("pipeline", pipeline);
        if (options.getMaxTime(MILLISECONDS) > 0) {
            commandDocument.put("maxTimeMS", options.getMaxTime(MILLISECONDS));
        }
        return commandDocument;
    }
}
