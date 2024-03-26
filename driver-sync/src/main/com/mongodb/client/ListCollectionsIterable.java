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

package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Iterable for ListCollections.
 *
 * @param <TResult> The type of the result.
 * @since 3.0
 * @mongodb.driver.manual reference/command/listCollections/ listCollections
 */
public interface ListCollectionsIterable<TResult> extends MongoIterable<TResult> {

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    ListCollectionsIterable<TResult> filter(@Nullable Bson filter);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     *
     * @deprecated Prefer using the operation execution timeout configuration options available at the following levels:
     *
     * <ul>
     *     <li>{@link MongoClientSettings.Builder#timeout(long, TimeUnit)}</li>
     *     <li>{@link MongoDatabase#withTimeout(long, TimeUnit)}</li>
     *     <li>{@link MongoCollection#withTimeout(long, TimeUnit)}</li>
     *     <li>{@link com.mongodb.ClientSessionOptions}</li>
     *     <li>{@link com.mongodb.TransactionOptions}</li>
     * </ul>
     *
     * When executing an operation, any explicitly set timeout at these levels takes precedence, rendering this maximum execution time
     * irrelevant. If no timeout is specified at these levels, the maximum execution time will be used.
     */
    @Deprecated
    ListCollectionsIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    @Override
    ListCollectionsIterable<TResult> batchSize(int batchSize);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    ListCollectionsIterable<TResult> comment(@Nullable String comment);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    ListCollectionsIterable<TResult> comment(@Nullable BsonValue comment);

    /**
     * Sets the timeoutMode for the cursor.
     *
     * <p>
     *     Requires the {@code timeout} to be set, either in the {@link com.mongodb.MongoClientSettings},
     *     via {@link MongoDatabase} or via {@link MongoCollection}
     * </p>
     * @param timeoutMode the timeout mode
     * @return this
     * @since CSOT
     */
    ListCollectionsIterable<TResult> timeoutMode(TimeoutMode timeoutMode);
}
