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

import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;

/**
 * <p>Finds the distinct values for a specified field across a single collection. This returns an array of the distinct values.</p>
 * 
 * <p>When possible, the distinct command uses an index to find documents and return values.</p>
 *
 * @mongodb.driver.manual reference/command/distinct Distinct Command
 * @since 3.0
 */
public class DistinctOperation implements AsyncReadOperation<BsonArray>, ReadOperation<BsonArray> {
    private final MongoNamespace namespace;
    private final String fieldName;
    private BsonDocument criteria;
    private long maxTimeMS;

    /**
     * Construct an instance.
     *
     * @param namespace the database and collection to run the query against
     * @param fieldName the field that needs to be distinct
     */
    public DistinctOperation(final MongoNamespace namespace, final String fieldName) {
        this.namespace = notNull("namespace", namespace);
        this.fieldName = notNull("fieldName", fieldName);
    }

    /**
     * Gets the query criteria.
     *
     * @return the query criteria
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Criteria
     */
    public BsonDocument getCriteria() {
        return criteria;
    }

    /**
     * Sets the criteria to apply to the query.
     *
     * @param criteria the criteria, which may be null.
     * @return this
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Criteria
     */
    public DistinctOperation criteria(final BsonDocument criteria) {
        this.criteria = criteria;
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
    public DistinctOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public BsonArray execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), binding, transformer());
    }

    @Override
    public MongoFuture<BsonArray> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), binding, transformer());
    }

    @SuppressWarnings("unchecked")
    private Function<BsonDocument, BsonArray> transformer() {
        return new Function<BsonDocument, BsonArray>() {
            @Override
            public BsonArray apply(final BsonDocument result) {
                return result.getArray("values");
            }
        };
    }

    private BsonDocument getCommand() {
        BsonDocument cmd = new BsonDocument("distinct", new BsonString(namespace.getCollectionName()));
        cmd.put("key", new BsonString(fieldName));
        putIfNotNull(cmd, "query", criteria);
        putIfNotZero(cmd, "maxTimeMS", maxTimeMS);

        return cmd;
    }
}
