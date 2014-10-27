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
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.MappedFieldNameValidator;
import com.mongodb.connection.NoOpFieldNameValidator;
import com.mongodb.connection.UpdateFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.DocumentHelper.putIfTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An operation that atomically finds and updates a single document.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class FindAndUpdateOperation<T> implements AsyncWriteOperation<T>, WriteOperation<T> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private final BsonDocument update;
    private BsonDocument criteria;
    private BsonDocument projection;
    private BsonDocument sort;
    private long maxTimeMS;
    private boolean returnUpdated = false;
    private boolean upsert = false;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param decoder the decoder for the result documents.
     * @param update the document containing update operators.
     */
    public FindAndUpdateOperation(final MongoNamespace namespace, final Decoder<T> decoder, final BsonDocument update) {
        this.namespace = notNull("namespace", namespace);
        this.decoder = notNull("decoder", decoder);
        this.update = notNull("decoder", update);
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
     * Gets the decoder used to decode the result documents.
     *
     * @return the decoder
     */
    public Decoder<T> getDecoder() {
        return decoder;
    }

    /**
     * Gets the document containing update operators
     *
     * @return the update document
     */
    public BsonDocument getUpdate() {
        return update;
    }

    /**
     * Gets the query criteria.
     *
     * @return the query criteria
     * @mongodb.driver.manual reference/method/db.collection.find/ Criteria
     */
    public BsonDocument getCriteria() {
        return criteria;
    }

    /**
     * Sets the criteria to apply to the query.
     *
     * @param criteria the criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Criteria
     */
    public FindAndUpdateOperation<T> criteria(final BsonDocument criteria) {
        this.criteria = criteria;
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
    public FindAndUpdateOperation<T> projection(final BsonDocument projection) {
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
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     */
    public FindAndUpdateOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
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
    public FindAndUpdateOperation<T> sort(final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    /**
     * When true if the updated document should be returned rather than the original. The default is false.
     *
     * @return true if the updated document should be returned, otherwise false
     */
    public boolean isReturnUpdated() {
        return returnUpdated;
    }

    /**
     * Set to true if the updated document should be returned rather than the original. The default is false.
     *
     * @param returnUpdated set to true if the updated document should be returned rather than the original.
     * @return this
     */
    public FindAndUpdateOperation<T> returnUpdated(final boolean returnUpdated) {
        this.returnUpdated = returnUpdated;
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
    public FindAndUpdateOperation<T> upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    @Override
    public T execute(final WriteBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), getValidator(),
                                             CommandResultDocumentCodec.create(decoder, "value"), binding,
                                             FindAndModifyHelper.<T>transformer());
    }

    @Override
    public MongoFuture<T> executeAsync(final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), getValidator(),
                                                  CommandResultDocumentCodec.create(decoder, "value"), binding,
                                                  FindAndModifyHelper.<T>transformer());
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument("findandmodify", new BsonString(namespace.getCollectionName()));
        putIfNotNull(command, "query", getCriteria());
        putIfNotNull(command, "fields", getProjection());
        putIfNotNull(command, "sort", getSort());
        putIfTrue(command, "new", isReturnUpdated());
        putIfTrue(command, "upsert", isUpsert());
        putIfNotZero(command, "maxTimeMS", getMaxTime(MILLISECONDS));
        command.put("update", getUpdate());
        return command;
    }

    private FieldNameValidator getValidator() {
        Map<String, FieldNameValidator> map = new HashMap<String, FieldNameValidator>();
        map.put("update", new UpdateFieldNameValidator());

        return new MappedFieldNameValidator(new NoOpFieldNameValidator(), map);
    }
}
