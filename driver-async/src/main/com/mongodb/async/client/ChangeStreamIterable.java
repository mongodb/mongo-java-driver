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

package com.mongodb.async.client;

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.concurrent.TimeUnit;

/**
 * Iterable for change streams.
 *
 * <p>Note: the {@link ChangeStreamDocument} class will not be applicable for all change stream outputs. If using custom pipelines that
 * radically change the result, then the {@link #withDocumentClass(Class)} method can be used to provide an alternative document format.</p>
 *
 * @param <TResult> The type of the result.
 * @mongodb.server.release 3.6
 * @since 3.6
 */
public interface ChangeStreamIterable<TResult> extends MongoIterable<ChangeStreamDocument<TResult>> {

    /**
     * Sets the fullDocument value.
     *
     * @param fullDocument the fullDocument
     * @return this
     */
    ChangeStreamIterable<TResult> fullDocument(FullDocument fullDocument);

    /**
     * Sets the logical starting point for the new change stream.
     *
     * @param resumeToken the resume token
     * @return this
     */
    ChangeStreamIterable<TResult> resumeAfter(BsonDocument resumeToken);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    ChangeStreamIterable<TResult> batchSize(int batchSize);

    /**
     * Sets the maximum await execution time on the server for this operation.
     *
     * @param maxAwaitTime  the max await time.  A zero value will be ignored, and indicates that the driver should respect the server's
     *                      default value
     * @param timeUnit the time unit, which may not be null
     * @return this
     */
    ChangeStreamIterable<TResult>  maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit);

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     */
    ChangeStreamIterable<TResult> collation(@Nullable Collation collation);

    /**
     * Returns a {@code MongoIterable} containing the results of the change stream based on the document class provided.
     *
     * @param clazz the class to use for the raw result.
     * @param <TDocument> the result type
     * @return the new Mongo Iterable
     */
    <TDocument> MongoIterable<TDocument> withDocumentClass(Class<TDocument> clazz);

    /**
     * The change stream will only provide changes that occurred at or after the specified timestamp.
     *
     * <p>Any command run against the server will return an operation time that can be used here.</p>
     * <p>The default value is an operation time obtained from the server before the change stream was created.</p>
     *
     * @param startAtOperationTime the start at operation time
     * @since 3.8
     * @return this
     * @mongodb.server.release 4.0
     * @mongodb.driver.manual reference/method/db.runCommand/
     */
    ChangeStreamIterable<TResult> startAtOperationTime(BsonTimestamp startAtOperationTime);
}
