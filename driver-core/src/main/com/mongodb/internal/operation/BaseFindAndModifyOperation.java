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
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableWriteAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite;
import static com.mongodb.internal.operation.OperationHelper.validateHintForFindAndModify;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableWrite;

/**
 * Abstract base class for findAndModify-based operations
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class BaseFindAndModifyOperation<T> implements WriteOperation<T> {
    private static final String COMMAND_NAME = "findAndModify";
    private final MongoNamespace namespace;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final Decoder<T> decoder;

    private BsonDocument filter;
    private BsonDocument projection;
    private BsonDocument sort;
    private Collation collation;
    private BsonDocument hint;
    private String hintString;
    private BsonValue comment;
    private BsonDocument variables;

    protected BaseFindAndModifyOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final boolean retryWrites,
            final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }


    @Override
    public T execute(final WriteBinding binding) {
        return executeRetryableWrite(binding, getDatabaseName(), null, getFieldNameValidator(),
                                     CommandResultDocumentCodec.create(getDecoder(), "value"),
                                     getCommandCreator(),
                                     FindAndModifyHelper.transformer(),
                                     cmd -> cmd);
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<T> callback) {
        executeRetryableWriteAsync(binding, getDatabaseName(), null, getFieldNameValidator(),
                                   CommandResultDocumentCodec.create(getDecoder(), "value"),
                                   getCommandCreator(),
                FindAndModifyHelper.asyncTransformer(), cmd -> cmd, callback);
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public Decoder<T> getDecoder() {
        return decoder;
    }

    public boolean isRetryWrites() {
        return retryWrites;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public BaseFindAndModifyOperation<T> filter(@Nullable final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public BsonDocument getProjection() {
        return projection;
    }

    public BaseFindAndModifyOperation<T> projection(@Nullable final BsonDocument projection) {
        this.projection = projection;
        return this;
    }

    public BsonDocument getSort() {
        return sort;
    }

    public BaseFindAndModifyOperation<T> sort(@Nullable final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    @Nullable
    public Collation getCollation() {
        return collation;
    }

    @Nullable
    public BsonDocument getHint() {
        return hint;
    }

    public BaseFindAndModifyOperation<T> hint(@Nullable final BsonDocument hint) {
        this.hint = hint;
        return this;
    }

    @Nullable
    public String getHintString() {
        return hintString;
    }

    public BaseFindAndModifyOperation<T> hintString(@Nullable final String hint) {
        this.hintString = hint;
        return this;
    }

    public BaseFindAndModifyOperation<T> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    public BsonValue getComment() {
        return comment;
    }

    public BaseFindAndModifyOperation<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    public BsonDocument getLet() {
        return variables;
    }

    public BaseFindAndModifyOperation<T> let(@Nullable final BsonDocument variables) {
        this.variables = variables;
        return this;
    }

    protected abstract FieldNameValidator getFieldNameValidator();

    protected abstract void specializeCommand(BsonDocument initialCommand, ConnectionDescription connectionDescription);

    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            SessionContext sessionContext = operationContext.getSessionContext();

            BsonDocument commandDocument = new BsonDocument(getCommandName(), new BsonString(getNamespace().getCollectionName()));
            putIfNotNull(commandDocument, "query", getFilter());
            putIfNotNull(commandDocument, "fields", getProjection());
            putIfNotNull(commandDocument, "sort", getSort());

            specializeCommand(commandDocument, connectionDescription);

            if (getWriteConcern().isAcknowledged() && !getWriteConcern().isServerDefault()
                    && !sessionContext.hasActiveTransaction()) {
                commandDocument.put("writeConcern", getWriteConcern().asDocument());
            }
            if (getCollation() != null) {
                commandDocument.put("collation", getCollation().asDocument());
            }
            if (getHint() != null || getHintString() != null) {
                validateHintForFindAndModify(connectionDescription, getWriteConcern());
                if (getHint() != null) {
                    commandDocument.put("hint", getHint());
                } else {
                    commandDocument.put("hint", new BsonString(getHintString()));
                }
            }
            putIfNotNull(commandDocument, "comment", getComment());
            putIfNotNull(commandDocument, "let", getLet());

            if (isRetryableWrite(isRetryWrites(), getWriteConcern(), connectionDescription, sessionContext)) {
                commandDocument.put("txnNumber", new BsonInt64(sessionContext.advanceTransactionNumber()));
            }
            return commandDocument;
        };
    }

    private String getDatabaseName() {
        return getNamespace().getDatabaseName();
    }
}
