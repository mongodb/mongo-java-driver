/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.validateReadConcern;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;

public class EstimatedDocumentCountOperation implements AsyncReadOperation<Long>, ReadOperation<Long> {
    private static final Decoder<BsonDocument> DECODER = new BsonDocumentCodec();
    private final MongoNamespace namespace;
    private boolean retryReads;
    private long maxTimeMS;

    public EstimatedDocumentCountOperation(final MongoNamespace namespace) {
        this.namespace = notNull("namespace", namespace);
    }

    public EstimatedDocumentCountOperation retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    public EstimatedDocumentCountOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public Long execute(final ReadBinding binding) {
        return executeCommand(binding, namespace.getDatabaseName(),
                getCommandCreator(binding.getSessionContext()), DECODER, transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Long> callback) {
        executeCommandAsync(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                DECODER, asyncTransformer(), retryReads, callback);
    }

    private CommandReadTransformer<BsonDocument, Long> transformer() {
        return (result, source, connection) -> (result.getNumber("n")).longValue();
    }

    private CommandReadTransformerAsync<BsonDocument, Long> asyncTransformer() {
        return (result, source, connection) -> (result.getNumber("n")).longValue();
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (serverDescription, connectionDescription) -> {
            validateReadConcern(connectionDescription, sessionContext.getReadConcern());
            return getCommand(sessionContext);
        };
    }

    private BsonDocument getCommand(final SessionContext sessionContext) {
        BsonDocument document = new BsonDocument("count", new BsonString(namespace.getCollectionName()));

        appendReadConcernToCommand(sessionContext, document);
        putIfNotZero(document, "maxTimeMS", maxTimeMS);
        return document;
    }
}
