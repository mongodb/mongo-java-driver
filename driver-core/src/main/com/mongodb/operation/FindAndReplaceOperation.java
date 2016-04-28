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

package com.mongodb.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.validator.CollectibleDocumentFieldNameValidator;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import com.mongodb.operation.OperationHelper.CallableWithConnection;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.DocumentHelper.putIfTrue;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.operation.OperationHelper.withConnection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An operation that atomically finds and replaces a single document.
 *
 * @param <T> the operations result type.
 * @since 3.0
 * @mongodb.driver.manual reference/command/findAndModify/ findAndModify
 */
public class FindAndReplaceOperation<T> implements AsyncWriteOperation<T>, WriteOperation<T> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private final BsonDocument replacement;
    private final WriteConcern writeConcern;
    private BsonDocument filter;
    private BsonDocument projection;
    private BsonDocument sort;
    private long maxTimeMS;
    private boolean returnOriginal = true;
    private boolean upsert;
    private Boolean bypassDocumentValidation;

    /**
     * Construct a new instance.
     *
     * @param namespace   the database and collection namespace for the operation.
     * @param decoder     the decoder for the result documents.
     * @param replacement the document that will replace the found document.
     * @deprecated use {@link #FindAndReplaceOperation(MongoNamespace, WriteConcern, Decoder, BsonDocument)} instead
     */
    @Deprecated
    public FindAndReplaceOperation(final MongoNamespace namespace, final Decoder<T> decoder, final BsonDocument replacement) {
        this(namespace, WriteConcern.ACKNOWLEDGED, decoder, replacement);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace   the database and collection namespace for the operation.
     * @param writeConcern the writeConcern for the operation
     * @param decoder     the decoder for the result documents.
     * @param replacement the document that will replace the found document.
     * @since 3.2
     */
    public FindAndReplaceOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final Decoder<T> decoder,
                                   final BsonDocument replacement) {
        this.namespace = notNull("namespace", namespace);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.decoder = notNull("decoder", decoder);
        this.replacement = notNull("replacement", replacement);
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
     * @return the {@link com.mongodb.WriteConcern}
     * @since 3.2
     * @mongodb.server.release 3.2
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
     * Gets the document which will replace the document matching the query filter.
     *
     * @return the replacement document
     */
    public BsonDocument getReplacement() {
        return replacement;
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
    public FindAndReplaceOperation<T> filter(final BsonDocument filter) {
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
    public FindAndReplaceOperation<T> projection(final BsonDocument projection) {
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
    public FindAndReplaceOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
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
    public FindAndReplaceOperation<T> sort(final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    /**
     * When false, returns the replaced document rather than the original. The default is false.
     *
     * @return true if the original document should be returned
     */
    public boolean isReturnOriginal() {
        return returnOriginal;
    }

    /**
     * Set to false to return the replaced document rather than the original.
     *
     * @param returnOriginal set to false to return the replaced document rather than the original
     * @return this
     */
    public FindAndReplaceOperation<T> returnOriginal(final boolean returnOriginal) {
        this.returnOriginal = returnOriginal;
        return this;
    }

    /**
     * Returns true if a new document should be inserted if there are no matches to the query filter.  The default is false.
     *
     * @return true if a new document should be inserted if there are no matches to the query filter
     */
    public boolean isUpsert() {
        return upsert;
    }

    /**
     * Set to true if a new document should be inserted if there are no matches to the query filter.
     *
     * @param upsert true if a new document should be inserted if there are no matches to the query filter
     * @return this
     */
    public FindAndReplaceOperation<T> upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    /**
     * Gets the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @since 3.2
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * <p>Note: This only applies when an $out stage is specified</p>.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 3.2
     */
    public FindAndReplaceOperation<T> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public T execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<T>() {
            @Override
            public T call(final Connection connection) {
                return executeWrappedCommandProtocol(binding, namespace.getDatabaseName(), asCommandDocument(connection.getDescription()),
                        getValidator(), CommandResultDocumentCodec.create(decoder, "value"), connection,
                        FindAndModifyHelper.<T>transformer());
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<T> callback) {
        withConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(), asCommandDocument(connection.getDescription()),
                            getValidator(), CommandResultDocumentCodec.create(decoder, "value"), connection,
                            FindAndModifyHelper.<T>transformer(), releasingCallback(errHandlingCallback, connection));
                }
            }
        });
    }

    private BsonDocument asCommandDocument(final ConnectionDescription description) {
        BsonDocument command = new BsonDocument("findandmodify", new BsonString(namespace.getCollectionName()));
        putIfNotNull(command, "query", getFilter());
        putIfNotNull(command, "fields", getProjection());
        putIfNotNull(command, "sort", getSort());
        putIfTrue(command, "new", !isReturnOriginal());
        putIfTrue(command, "upsert", isUpsert());
        putIfNotZero(command, "maxTimeMS", getMaxTime(MILLISECONDS));
        command.put("update", getReplacement());
        if (bypassDocumentValidation != null && serverIsAtLeastVersionThreeDotTwo(description)) {
            command.put("bypassDocumentValidation", BsonBoolean.valueOf(bypassDocumentValidation));
        }
        if (serverIsAtLeastVersionThreeDotTwo(description) && writeConcern.isAcknowledged() && !writeConcern.isServerDefault()) {
            command.put("writeConcern", writeConcern.asDocument());
        }
        return command;
    }

    private FieldNameValidator getValidator() {
        Map<String, FieldNameValidator> map = new HashMap<String, FieldNameValidator>();
        map.put("update", new CollectibleDocumentFieldNameValidator());
        return new MappedFieldNameValidator(new NoOpFieldNameValidator(), map);
    }
}
