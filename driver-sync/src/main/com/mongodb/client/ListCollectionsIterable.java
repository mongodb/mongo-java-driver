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

import com.mongodb.lang.Nullable;
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
     * Sets the {@code authorizedCollections} field of the {@code listCollections} command.
     * This method is ignored if called on a {@link ListCollectionsIterable} obtained not via any of the
     * {@link MongoDatabase#listCollectionNames() MongoDatabase.listCollectionNames} methods.
     *
     * @param authorizedCollections If {@code true}, allows executing the {@code listCollections} command,
     * which has the {@code nameOnly} field set to {@code true}, without having the
     * <a href="https://docs.mongodb.com/manual/reference/privilege-actions/#mongodb-authaction-listCollections">
     * {@code listCollections} privilege</a> on the corresponding database resource.
     * @return {@code this}.
     * @since 4.5
     * @mongodb.server.release 4.0
     */
    ListCollectionsIterable<TResult> authorizedCollections(boolean authorizedCollections);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
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
}
