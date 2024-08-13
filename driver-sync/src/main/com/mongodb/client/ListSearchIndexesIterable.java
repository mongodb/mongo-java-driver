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

import com.mongodb.ExplainVerbosity;
import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Evolving;
import com.mongodb.annotations.Reason;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.concurrent.TimeUnit;
/**
 * Iterable for listing Atlas Search indexes.
 * This interface contains aggregate options and that of applied to {@code $listSearchIndexes} operation.
 *
 * @param <TResult> The type of the result.
 * @mongodb.driver.manual reference/operator/aggregation/listSearchIndexes ListSearchIndexes
 * @since 4.11
 * @mongodb.server.release 6.0
 */
@Evolving
public interface ListSearchIndexesIterable<TResult> extends MongoIterable<TResult> {

    /**
     * Sets the index name for this operation.
     *
     * @param indexName the index name.
     * @return this.
     */
    ListSearchIndexesIterable<TResult> name(String indexName);

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled.
     * @return this.
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    ListSearchIndexesIterable<TResult> allowDiskUse(@Nullable Boolean allowDiskUse);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size.
     * @return this.
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    ListSearchIndexesIterable<TResult> batchSize(int batchSize);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time.
     * @param timeUnit the time unit, which may not be null.
     * @return this.
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    ListSearchIndexesIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     *
     * @param collation the collation options to use
     * @return this
     */
    ListSearchIndexesIterable<TResult> collation(@Nullable Collation collation);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment.
     * @return this
     */
    ListSearchIndexesIterable<TResult> comment(@Nullable String comment);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment.
     * @return this.
     */
    ListSearchIndexesIterable<TResult> comment(@Nullable BsonValue comment);

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
    ListSearchIndexesIterable<TResult> timeoutMode(TimeoutMode timeoutMode);

    /**
     * Explain the execution plan for this operation with the server's default verbosity level.
     *
     * @return the execution plan.
     * @mongodb.driver.manual reference/command/explain/
     */
    Document explain();

    /**
     * Explain the execution plan for this operation with the given verbosity level.
     *
     * @param verbosity the verbosity of the explanation.
     * @return the execution plan.
     * @mongodb.driver.manual reference/command/explain/
     */
    Document explain(ExplainVerbosity verbosity);

    /**
     * Explain the execution plan for this operation with the server's default verbosity level.
     *
     * @param <E>                the type of the document class.
     * @param explainResultClass the document class to decode into.
     * @return the execution plan.
     * @mongodb.driver.manual reference/command/explain/
     */
    <E> E explain(Class<E> explainResultClass);

    /**
     * Explain the execution plan for this operation with the given verbosity level.
     *
     * @param <E>                the type of the document class.
     * @param explainResultClass the document class to decode into.
     * @param verbosity          the verbosity of the explanation.
     * @return the execution plan.
     * @mongodb.driver.manual reference/command/explain/
     */
    <E> E explain(Class<E> explainResultClass, ExplainVerbosity verbosity);
}
