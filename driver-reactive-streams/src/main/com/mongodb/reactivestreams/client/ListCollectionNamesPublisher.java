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

import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * Publisher for listing collection names.
 *
 * @since 5.0
 * @mongodb.driver.manual reference/command/listCollections/ listCollections
 */
public interface ListCollectionNamesPublisher extends Publisher<String> {
    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    ListCollectionNamesPublisher filter(@Nullable Bson filter);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    ListCollectionNamesPublisher maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * Sets the number of documents to return per batch.
     *
     * <p>Overrides the {@link org.reactivestreams.Subscription#request(long)} value for setting the batch size, allowing for fine-grained
     * control over the underlying cursor.</p>
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    ListCollectionNamesPublisher batchSize(int batchSize);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @mongodb.server.release 4.4
     */
    ListCollectionNamesPublisher comment(@Nullable String comment);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @mongodb.server.release 4.4
     */
    ListCollectionNamesPublisher comment(@Nullable BsonValue comment);

    /**
     * Helper to return a publisher limited to the first result.
     *
     * @return a Publisher which will contain a single item.
     */
    Publisher<String> first();

    /**
     * Sets the {@code authorizedCollections} field of the {@code listCollections} command.
     *
     * @param authorizedCollections If {@code true}, allows executing the {@code listCollections} command,
     * which has the {@code nameOnly} field set to {@code true}, without having the
     * <a href="https://docs.mongodb.com/manual/reference/privilege-actions/#mongodb-authaction-listCollections">
     * {@code listCollections} privilege</a> on the database resource.
     * @return {@code this}.
     * @mongodb.server.release 4.0
     */
    ListCollectionNamesPublisher authorizedCollections(boolean authorizedCollections);
}
