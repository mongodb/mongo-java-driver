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

package com.mongodb.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.session.SessionContext;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.OperationHelper.validateCollation;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An operation that atomically finds and deletes a single document.
 *
 * @param <T> the operations result type.
 * @mongodb.driver.manual reference/command/findAndModify/ findAndModify
 * @since 3.0
 */
public class FindAndDeleteOperation<T> extends BaseFindAndModifyOperation<T> {
    private BsonDocument filter;
    private BsonDocument projection;
    private BsonDocument sort;
    private long maxTimeMS;
    private Collation collation;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param decoder   the decoder for the result documents.
     * @deprecated      use {@link #FindAndDeleteOperation(MongoNamespace, WriteConcern, boolean, Decoder)} instead
     */
    @Deprecated
    public FindAndDeleteOperation(final MongoNamespace namespace, final Decoder<T> decoder) {
        this(namespace, WriteConcern.ACKNOWLEDGED, false, decoder);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param writeConcern the writeConcern for the operation
     * @param decoder      the decoder for the result documents.
     * @since 3.2
     * @deprecated         use {@link #FindAndDeleteOperation(MongoNamespace, WriteConcern, boolean, Decoder)} instead
     */
    @Deprecated
    public FindAndDeleteOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final Decoder<T> decoder) {
        this(namespace, writeConcern, false, decoder);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param writeConcern the writeConcern for the operation
     * @param retryWrites  if writes should be retried if they fail due to a network error.
     * @param decoder      the decoder for the result documents.
     * @since 3.6
     */
    public FindAndDeleteOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final boolean retryWrites,
                                  final Decoder<T> decoder) {
        super(namespace, writeConcern, retryWrites, decoder);
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
     * Sets the filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public FindAndDeleteOperation<T> filter(final BsonDocument filter) {
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
    public FindAndDeleteOperation<T> projection(final BsonDocument projection) {
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
    public FindAndDeleteOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
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
    public FindAndDeleteOperation<T> sort(final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public FindAndDeleteOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    protected String getDatabaseName() {
        return getNamespace().getDatabaseName();
    }

    @Override
    protected CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                validateCollation(connectionDescription, collation);
                BsonDocument commandDocument = new BsonDocument("findAndModify", new BsonString(getNamespace().getCollectionName()));
                putIfNotNull(commandDocument, "query", getFilter());
                putIfNotNull(commandDocument, "fields", getProjection());
                putIfNotNull(commandDocument, "sort", getSort());
                putIfNotZero(commandDocument, "maxTimeMS", getMaxTime(MILLISECONDS));
                commandDocument.put("remove", BsonBoolean.TRUE);
                addWriteConcernToCommand(connectionDescription, commandDocument, sessionContext);
                if (collation != null) {
                    commandDocument.put("collation", collation.asDocument());
                }
                addTxnNumberToCommand(serverDescription, connectionDescription, commandDocument, sessionContext);
                return commandDocument;
            }
        };
    }

    @Override
    protected FieldNameValidator getFieldNameValidator() {
        return new NoOpFieldNameValidator();
    }

}
