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

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Iterable for ListDatabases.
 *
 * @param <TResult> The type of the result.
 * @since 3.0
 */
public interface ListDatabasesIterable<TResult> extends MongoIterable<TResult> {

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    ListDatabasesIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    @Override
    ListDatabasesIterable<TResult> batchSize(int batchSize);

    /**
     * Sets the query filter to apply to the returned database names.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.4.2
     */
    ListDatabasesIterable<TResult> filter(@Nullable Bson filter);

    /**
     * Sets the nameOnly flag that indicates whether the command should return just the database names or return the database names and
     * size information.
     *
     * @param nameOnly the nameOnly flag, which may be null
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.4.3
     */
    ListDatabasesIterable<TResult> nameOnly(@Nullable Boolean nameOnly);

    /**
     * Sets the authorizedDatabasesOnly flag that indicates whether the command should return just the databases which the user
     * is authorized to see.
     *
     * @param authorizedDatabasesOnly the authorizedDatabasesOnly flag, which may be null
     * @return this
     * @since 4.1
     * @mongodb.server.release 4.0
     */
    ListDatabasesIterable<TResult> authorizedDatabasesOnly(@Nullable Boolean authorizedDatabasesOnly);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    ListDatabasesIterable<TResult> comment(@Nullable String comment);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    ListDatabasesIterable<TResult> comment(@Nullable BsonValue comment);

    /**
     * Sets the timeoutMode for the cursor.
     *
     * <p>
     *     Requires the {@code timeout} to be set, either in the {@link com.mongodb.MongoClientSettings},
     *     via {@link MongoDatabase} or via {@link MongoCollection}
     * </p>
     * @param timeoutMode the timeout mode
     * @return this
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    ListDatabasesIterable<TResult> timeoutMode(TimeoutMode timeoutMode);
}
