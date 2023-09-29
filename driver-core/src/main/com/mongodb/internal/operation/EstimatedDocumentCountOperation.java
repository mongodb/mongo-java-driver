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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandReadTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandReadTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;
import static java.util.Collections.singletonList;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class EstimatedDocumentCountOperation implements AsyncReadOperation<Long>, ReadOperation<Long> {
    private static final Decoder<BsonDocument> DECODER = new BsonDocumentCodec();
    private final TimeoutSettings timeoutSettings;
    private final TimeoutContext timeoutContext;
    private final MongoNamespace namespace;
    private boolean retryReads;
    private BsonValue comment;

    public EstimatedDocumentCountOperation(final TimeoutSettings timeoutSettings, final MongoNamespace namespace) {
        this.timeoutSettings = timeoutSettings;
        this.timeoutContext = new TimeoutContext(timeoutSettings);
        this.namespace = notNull("namespace", namespace);
    }

    public EstimatedDocumentCountOperation retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    public EstimatedDocumentCountOperation comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    @Override
    public Long execute(final ReadBinding binding) {
        try {
            return executeRetryableRead(timeoutContext, binding, namespace.getDatabaseName(),
                                        getCommandCreator(binding.getSessionContext()), CommandResultDocumentCodec.create(DECODER, singletonList("firstBatch")),
                                        transformer(), retryReads);
        } catch (MongoCommandException e) {
            return assertNotNull(rethrowIfNotNamespaceError(e, 0L));
        }
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Long> callback) {
        executeRetryableReadAsync(timeoutContext, binding, namespace.getDatabaseName(),
                                  getCommandCreator(binding.getSessionContext()), CommandResultDocumentCodec.create(DECODER, singletonList("firstBatch")),
                                  asyncTransformer(), retryReads,
                                  (result, t) -> {
                    if (isNamespaceError(t)) {
                        callback.onResult(0L, null);
                    } else {
                        callback.onResult(result, t);
                    }
                });
    }

    private CommandReadTransformer<BsonDocument, Long> transformer() {
        return (result, source, connection) -> transformResult(result, connection.getDescription());
    }

    private CommandReadTransformerAsync<BsonDocument, Long> asyncTransformer() {
        return (result, source, connection) -> transformResult(result, connection.getDescription());
    }

    private long transformResult(final BsonDocument result, final ConnectionDescription connectionDescription) {
        return (result.getNumber("n")).longValue();
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (timeoutContext, serverDescription, connectionDescription) -> {
            BsonDocument document = new BsonDocument("count", new BsonString(namespace.getCollectionName()));
            appendReadConcernToCommand(sessionContext, connectionDescription.getMaxWireVersion(), document);
            putIfNotZero(document, "maxTimeMS", timeoutContext.getMaxTimeMS());
            if (comment != null) {
                document.put("comment", comment);
            }
            return document;
        };
    }
}
