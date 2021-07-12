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


package com.mongodb.reactivestreams.client;

import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * Publisher interface for ListDatabases.
 *
 * @param <TResult> The type of the result.
 * @since 1.0
 */
public interface ListDatabasesPublisher<TResult> extends Publisher<TResult> {

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    ListDatabasesPublisher<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * Sets the query filter to apply to the returned database names.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.server.release 3.4.2
     * @since 1.7
     */
    ListDatabasesPublisher<TResult> filter(@Nullable Bson filter);

    /**
     * Sets the nameOnly flag that indicates whether the command should return just the database names or return the database names and
     * size information.
     *
     * @param nameOnly the nameOnly flag, which may be null
     * @return this
     * @mongodb.server.release 3.4.3
     * @since 1.7
     */
    ListDatabasesPublisher<TResult> nameOnly(@Nullable Boolean nameOnly);

    /**
     * Sets the authorizedDatabasesOnly flag that indicates whether the command should return just the databases which the user
     * is authorized to see.
     *
     * @param authorizedDatabasesOnly the authorizedDatabasesOnly flag, which may be null
     * @return this
     * @since 4.1
     * @mongodb.server.release 4.0
     */
    ListDatabasesPublisher<TResult> authorizedDatabasesOnly(@Nullable Boolean authorizedDatabasesOnly);

    /**
     * Sets the number of documents to return per batch.
     *
     * <p>Overrides the {@link org.reactivestreams.Subscription#request(long)} value for setting the batch size, allowing for fine-grained
     * control over the underlying cursor.</p>
     *
     * @param batchSize the batch size
     * @return this
     * @since 1.8
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    ListDatabasesPublisher<TResult> batchSize(int batchSize);

    /**
     * Helper to return a publisher limited to the first result.
     *
     * @return a Publisher which will contain a single item.
     * @since 1.8
     */
    Publisher<TResult> first();
}
