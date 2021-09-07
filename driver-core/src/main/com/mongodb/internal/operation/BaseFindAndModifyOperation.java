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
import com.mongodb.WriteConcern;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableWrite;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableWriteAsync;
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;

/**
 * Abstract base class for findAndModify-based operations
 *
 * @param <T> the document type
 * @since 3.8
 */
public abstract class BaseFindAndModifyOperation<T> implements AsyncWriteOperation<T>, WriteOperation<T> {

    private final MongoNamespace namespace;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final Decoder<T> decoder;

    /**
     * Construct a new instance.
     *
     * @param namespace   the database and collection namespace for the operation.
     * @param writeConcern the writeConcern for the operation
     * @param retryWrites  if writes should be retried if they fail due to a network error.
     * @param decoder     the decoder for the result documents.
     */
    protected BaseFindAndModifyOperation(final MongoNamespace namespace, final WriteConcern writeConcern,
                                         final boolean retryWrites, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public T execute(final WriteBinding binding) {
        return executeRetryableWrite(binding, getDatabaseName(), null, getFieldNameValidator(),
                CommandResultDocumentCodec.create(getDecoder(), "value"),
                getCommandCreator(binding.getSessionContext()),
                FindAndModifyHelper.transformer(),
                cmd -> cmd);
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<T> callback) {
        executeRetryableWriteAsync(binding, getDatabaseName(), null, getFieldNameValidator(),
                CommandResultDocumentCodec.create(getDecoder(), "value"),
                getCommandCreator(binding.getSessionContext()), FindAndModifyHelper.asyncTransformer(), cmd -> cmd, callback);
    }

    protected abstract String getDatabaseName();

    /**
     * Gets the namespace.
     *
     * @return the namespace
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Get the write concern for this operation
     *
     * @return the {@link WriteConcern}
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets the decoder used to decode the result documents.
     *
     * @return the decoder
     */
    public Decoder<T> getDecoder() {
        return decoder;
    }

    /**
     * Returns true if the operation should be retried.
     *
     * @return true if the operation should be retried
     * @since 3.8
     */
    public boolean isRetryWrites() {
        return retryWrites;
    }

    protected abstract CommandCreator getCommandCreator(SessionContext sessionContext);

    protected void addTxnNumberToCommand(final ServerDescription serverDescription, final ConnectionDescription connectionDescription,
                                         final BsonDocument commandDocument, final SessionContext sessionContext) {
        if (isRetryableWrite(isRetryWrites(), getWriteConcern(), serverDescription, connectionDescription, sessionContext)) {
            commandDocument.put("txnNumber", new BsonInt64(sessionContext.advanceTransactionNumber()));
        }
    }

    protected void addWriteConcernToCommand(final ConnectionDescription connectionDescription, final BsonDocument commandDocument,
                                            final SessionContext sessionContext) {
        if (getWriteConcern().isAcknowledged() && !getWriteConcern().isServerDefault()
                && serverIsAtLeastVersionThreeDotTwo(connectionDescription) && !sessionContext.hasActiveTransaction()) {
            commandDocument.put("writeConcern", getWriteConcern().asDocument());
        }
    }

    protected abstract FieldNameValidator getFieldNameValidator();
}
