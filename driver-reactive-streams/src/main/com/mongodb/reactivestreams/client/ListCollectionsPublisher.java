/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * Publisher interface for ListCollections.
 *
 * @param <TResult> The type of the result.
 * @since 1.0
 * @mongodb.driver.manual reference/command/listCollections/ listCollections
 */
public interface ListCollectionsPublisher<TResult> extends Publisher<TResult> {

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    ListCollectionsPublisher<TResult> filter(@Nullable Bson filter);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    ListCollectionsPublisher<TResult> maxTime(long maxTime, TimeUnit timeUnit);

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
    ListCollectionsPublisher<TResult> batchSize(int batchSize);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    ListCollectionsPublisher<TResult> comment(@Nullable String comment);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    ListCollectionsPublisher<TResult> comment(@Nullable BsonValue comment);

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
    ListCollectionsPublisher<TResult> timeoutMode(TimeoutMode timeoutMode);

    /**
     * Helper to return a publisher limited to the first result.
     *
     * @return a Publisher which will contain a single item.
     * @since 1.8
     */
    Publisher<TResult> first();
}
