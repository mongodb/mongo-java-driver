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

import org.mongodb.AggregationOptions;
import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.session.Session;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;

abstract class AggregateBaseOperation<T> {
    private static final Logger LOGGER = Loggers.getLogger("operation");

    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private final List<Document> pipeline;
    private final AggregationOptions options;
    private final ReadPreference readPreference;

    protected AggregateBaseOperation(final MongoNamespace namespace, final List<Document> pipeline, final Decoder<T> decoder,
                              final AggregationOptions options, final ReadPreference readPreference) {
        this.namespace = namespace;
        this.decoder = decoder;
        this.pipeline = pipeline;
        this.options = options;
        this.readPreference = readPreference;
    }

    protected CommandResult sendAndReceiveMessage(final Session session) {
        return executeWrappedCommandProtocol(namespace, asCommandDocument(), new DocumentCodec(),
                                             new CommandResultWithPayloadDecoder<T>(decoder, "result"),
                                             readPreference, session);
    }

    protected MongoNamespace getNamespace() {
        return namespace;
    }

    protected Decoder<T> getDecoder() {
        return decoder;
    }

    protected AggregationOptions getOptions() {
        return options;
    }

    protected ReadPreference getReadPreference() {
        return readPreference;
    }

    protected Document asCommandDocument() {
        Document commandDocument = new Document("aggregate", namespace.getCollectionName());
        commandDocument.put("pipeline", pipeline);
        if (options.getMaxTime(MILLISECONDS) > 0) {
            commandDocument.put("maxTimeMS", options.getMaxTime(MILLISECONDS));
        }
        if (options.getOutputMode() == AggregationOptions.OutputMode.CURSOR) {
            Document cursor = new Document();
            if (options.getBatchSize() != null) {
                cursor.put("batchSize", options.getBatchSize());
            }
            commandDocument.put("cursor", cursor);
        }
        if (options.getAllowDiskUse() != null) {
            commandDocument.put("allowDiskUse", options.getAllowDiskUse());
        }
        return commandDocument;
    }
}
