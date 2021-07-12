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
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableWrite;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableWriteAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite;
import static com.mongodb.internal.operation.OperationHelper.validateHintForFindAndModify;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

    private BsonDocument filter;
    private BsonDocument projection;
    private BsonDocument sort;
    private long maxTimeMS;
    private Collation collation;
    private Bson hint;
    private String hintString;
    private BsonValue comment;
    private BsonDocument variables;

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


    /**
     * Gets the query filter.
     *
     * @return the query filter
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the query filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public BaseFindAndModifyOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public BsonDocument getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public BaseFindAndModifyOperation<T> projection(final BsonDocument projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     */
    public BaseFindAndModifyOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public BsonDocument getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public BaseFindAndModifyOperation<T> sort(final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Returns the hint for which index to use. The default is not to set a hint.
     *
     * @return the hint
     * @since 4.1
     */
    @Nullable
    public Bson getHint() {
        return hint;
    }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 4.1
     */
    public BaseFindAndModifyOperation<T> hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Gets the hint string to apply.
     *
     * @return the hint string, which should be the name of an existing index
     * @since 4.1
     */
    @Nullable
    public String getHintString() {
        return hintString;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     * @since 4.1
     */
    public BaseFindAndModifyOperation<T> hintString(@Nullable final String hint) {
        this.hintString = hint;
        return this;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public BaseFindAndModifyOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * @return comment for this operation. A null value means no comment is set.
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    public BsonValue getComment() {
        return comment;
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    public BaseFindAndModifyOperation<T> comment(final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    /**
     * Add top-level variables to the operation
     *
     * @return the top level variables if set or null.
     * @mongodb.server.release 5.0
     * @since 4.6
     */
    public BsonDocument getLet() {
        return variables;
    }

    /**
     * Add top-level variables for the operation
     *
     * <p>Allows for improved command readability by separating the variables from the query text.
     *
     * @param variables for the operation
     * @return this
     * @mongodb.server.release 5.0
     * @since 4.6
     */
    public BaseFindAndModifyOperation<T> let(final BsonDocument variables) {
        this.variables = variables;
        return this;
    }

    protected abstract FieldNameValidator getFieldNameValidator();

    protected abstract void specializeCommand(BsonDocument initialCommand, ConnectionDescription connectionDescription);

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (serverDescription, connectionDescription) -> {
            BsonDocument commandDocument = new BsonDocument("findAndModify", new BsonString(getNamespace().getCollectionName()));
            putIfNotNull(commandDocument, "query", getFilter());
            putIfNotNull(commandDocument, "fields", getProjection());
            putIfNotNull(commandDocument, "sort", getSort());

            specializeCommand(commandDocument, connectionDescription);

            putIfNotZero(commandDocument, "maxTimeMS", getMaxTime(MILLISECONDS));
            if (getWriteConcern().isAcknowledged() && !getWriteConcern().isServerDefault() && !sessionContext.hasActiveTransaction()) {
                commandDocument.put("writeConcern", getWriteConcern().asDocument());
            }
            if (getCollation() != null) {
                commandDocument.put("collation", getCollation().asDocument());
            }
            if (getHint() != null || getHintString() != null) {
                validateHintForFindAndModify(connectionDescription, getWriteConcern());
                if (getHint() != null) {
                    commandDocument.put("hint", getHint().toBsonDocument(BsonDocument.class, null));
                } else {
                    commandDocument.put("hint", new BsonString(getHintString()));
                }
            }
            putIfNotNull(commandDocument, "comment", getComment());
            putIfNotNull(commandDocument, "let", getLet());

            if (isRetryableWrite(isRetryWrites(), getWriteConcern(), serverDescription, connectionDescription, sessionContext)) {
                commandDocument.put("txnNumber", new BsonInt64(sessionContext.advanceTransactionNumber()));
            }
            return commandDocument;
        };
    }

    private String getDatabaseName() {
        return getNamespace().getDatabaseName();
    }
}
