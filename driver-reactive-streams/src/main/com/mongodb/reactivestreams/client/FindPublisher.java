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

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * Publisher interface for find.
 *
 * @param <TResult> The type of the result.
 * @since 1.0
 */
public interface FindPublisher<TResult> extends Publisher<TResult> {

    /**
     * Helper to return a publisher limited to the first result.
     *
     * @return a Publisher which will contain a single item.
     */
    Publisher<TResult> first();

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    FindPublisher<TResult> filter(@Nullable Bson filter);

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    FindPublisher<TResult> limit(int limit);
    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @return this
     * @mongodb.driver.manual reference/method/cursor.skip/#cursor.skip Skip
     */
    FindPublisher<TResult> skip(int skip);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    FindPublisher<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a tailable cursor
     * query. This only applies to a TAILABLE_AWAIT cursor. When the cursor is not a TAILABLE_AWAIT cursor,
     * this option is ignored.
     * <p>
     * On servers &gt;= 3.2, this option will be specified on the getMore command as "maxTimeMS". The default
     * is no value: no "maxTimeMS" is sent to the server with the getMore command.
     * <p>
     * On servers &lt; 3.2, this option is ignored, and indicates that the driver should respect the server's default value
     * <p>
     * A zero value will be ignored.
     *
     * @param maxAwaitTime  the max await time
     * @param timeUnit the time unit to return the result in
     * @return the maximum await execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     * @since 1.2
     */
    FindPublisher<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit);

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    FindPublisher<TResult> projection(@Nullable Bson projection);
    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    FindPublisher<TResult> sort(@Nullable Bson sort);

    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes)
     * to prevent excess memory use. Set this option to prevent that.
     *
     * @param noCursorTimeout true if cursor timeout is disabled
     * @return this
     */
    FindPublisher<TResult> noCursorTimeout(boolean noCursorTimeout);

    /**
     * Users should not set this under normal circumstances.
     *
     * @param oplogReplay if oplog replay is enabled
     * @return this
     * @deprecated oplogReplay has been deprecated in MongoDB 4.4.
     */
    @Deprecated
    FindPublisher<TResult> oplogReplay(boolean oplogReplay);

    /**
     * Get partial results from a sharded cluster if one or more shards are unreachable (instead of throwing an error).
     *
     * @param partial if partial results for sharded clusters is enabled
     * @return this
     */
    FindPublisher<TResult> partial(boolean partial);

    /**
     * Sets the cursor type.
     *
     * @param cursorType the cursor type
     * @return this
     */
    FindPublisher<TResult> cursorType(CursorType cursorType);

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 1.3
     * @mongodb.server.release 3.4
     */
    FindPublisher<TResult> collation(@Nullable Collation collation);

    /**
     * Sets the comment to the query. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 1.6
     */
    FindPublisher<TResult> comment(@Nullable String comment);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * <p>The comment can be any valid BSON type for server versions 4.4 and above.
     * Server versions between 3.6 and 4.2 only support string as comment,
     * and providing a non-string type will result in a server-side error.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 3.6
     */
    FindPublisher<TResult> comment(@Nullable BsonValue comment);

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 1.6
     */
    FindPublisher<TResult> hint(@Nullable Bson hint);

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     * @since 1.13
     */
    FindPublisher<TResult> hintString(@Nullable String hint);

    /**
     * Add top-level variables to the operation. A null value means no variables are set.
     *
     * <p>Allows for improved command readability by separating the variables from the query text.</p>
     *
     * @param variables for find operation or null
     * @return this
     * @mongodb.driver.manual reference/command/find/
     * @mongodb.server.release 5.0
     * @since 4.6
     */
    FindPublisher<TResult> let(@Nullable Bson variables);

    /**
     * Sets the exclusive upper bound for a specific index. A null value means no max is set.
     *
     * @param max the max
     * @return this
     * @since 1.6
     */
    FindPublisher<TResult> max(@Nullable Bson max);

    /**
     * Sets the minimum inclusive lower bound for a specific index. A null value means no max is set.
     *
     * @param min the min
     * @return this
     * @since 1.6
     */
    FindPublisher<TResult> min(@Nullable Bson min);

    /**
     * Sets the returnKey. If true the find operation will return only the index keys in the resulting documents.
     *
     * @param returnKey the returnKey
     * @return this
     * @since 1.6
     */
    FindPublisher<TResult> returnKey(boolean returnKey);

    /**
     * Sets the showRecordId. Set to true to add a field {@code $recordId} to the returned documents.
     *
     * @param showRecordId the showRecordId
     * @return this
     * @since 1.6
     */
    FindPublisher<TResult> showRecordId(boolean showRecordId);

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
    FindPublisher<TResult> batchSize(int batchSize);

    /**
     * Enables writing to temporary files on the server. When set to true, the server
     * can write temporary data to disk while executing the find operation.
     * <p>
     * This option is sent only if the caller explicitly sets it to true.
     *
     * @param allowDiskUse the allowDiskUse
     * @return this
     * @since 4.1
     * @mongodb.server.release 4.4
     */
    FindPublisher<TResult> allowDiskUse(@Nullable Boolean allowDiskUse);

    /**
     * Explain the execution plan for this operation with the server's default verbosity level
     *
     * @return the execution plan
     * @since 4.2
     * @mongodb.driver.manual reference/command/explain/
     * @mongodb.server.release 3.2
     */
    Publisher<Document> explain();

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param verbosity the verbosity of the explanation
     * @return the execution plan
     * @since 4.2
     * @mongodb.driver.manual reference/command/explain/
     * @mongodb.server.release 3.2
     */
    Publisher<Document> explain(ExplainVerbosity verbosity);

    /**
     * Explain the execution plan for this operation with the server's default verbosity level
     *
     * @param <E> the type of the document class
     * @param explainResultClass the document class to decode into
     * @return the execution plan
     * @since 4.2
     * @mongodb.driver.manual reference/command/explain/
     * @mongodb.server.release 3.2
     */
    <E> Publisher<E> explain(Class<E> explainResultClass);

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param <E> the type of the document class
     * @param explainResultClass the document class to decode into
     * @param verbosity            the verbosity of the explanation
     * @return the execution plan
     * @since 4.2
     * @mongodb.driver.manual reference/command/explain/
     * @mongodb.server.release 3.2
     */
    <E> Publisher<E> explain(Class<E> explainResultClass, ExplainVerbosity verbosity);
}
