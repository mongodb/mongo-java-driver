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
 *
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * Iterable for change streams.
 *
 * @param <TResult> The type of the result.
 * @mongodb.server.release 3.6
 * @since 1.6
 */
public interface ChangeStreamPublisher<TResult> extends Publisher<ChangeStreamDocument<TResult>> {
    /**
     * Sets the fullDocument value.
     *
     * @param fullDocument the fullDocument
     * @return this
     */
    ChangeStreamPublisher<TResult> fullDocument(FullDocument fullDocument);

    /**
     * Sets the logical starting point for the new change stream.
     *
     * @param resumeToken the resume token
     * @return this
     */
    ChangeStreamPublisher<TResult> resumeAfter(BsonDocument resumeToken);

    /**
     * The change stream will only provide changes that occurred after the specified timestamp.
     *
     * <p>Any command run against the server will return an operation time that can be used here.</p>
     * <p>The default value is an operation time obtained from the server before the change stream was created.</p>
     *
     * @param startAtOperationTime the start at operation time
     * @since 1.9
     * @return this
     * @mongodb.server.release 4.0
     * @mongodb.driver.manual reference/method/db.runCommand/
     */
    ChangeStreamPublisher<TResult> startAtOperationTime(BsonTimestamp startAtOperationTime);

    /**
     * Similar to {@code resumeAfter}, this option takes a resume token and starts a
     * new change stream returning the first notification after the token.
     *
     * <p>This will allow users to watch collections that have been dropped and recreated
     * or newly renamed collections without missing any notifications.</p>
     *
     * <p>Note: The server will report an error if both {@code startAfter} and {@code resumeAfter} are specified.</p>
     *
     * @param startAfter the startAfter resumeToken
     * @return this
     * @since 1.12
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual changeStreams/#change-stream-start-after
     */
    ChangeStreamPublisher<TResult> startAfter(BsonDocument startAfter);

    /**
     * Sets the maximum await execution time on the server for this operation.
     *
     * @param maxAwaitTime  the max await time.  A zero value will be ignored, and indicates that the driver should respect the server's
     *                      default value
     * @param timeUnit the time unit, which may not be null
     * @return this
     */
    ChangeStreamPublisher<TResult>  maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit);

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     */
    ChangeStreamPublisher<TResult> collation(@Nullable Collation collation);

    /**
     * Returns a {@code MongoIterable} containing the results of the change stream based on the document class provided.
     *
     * @param clazz the class to use for the raw result.
     * @param <TDocument> the result type
     * @return the new Mongo Iterable
     */
    <TDocument> Publisher<TDocument> withDocumentClass(Class<TDocument> clazz);

    /**
     * Sets the number of documents to return per batch.
     *
     * <p>Overrides the {@link org.reactivestreams.Subscription#request(long)} value for setting the batch size, allowing for fine grained
     * control over the underlying cursor.</p>
     *
     * @param batchSize the batch size
     * @return this
     * @since 1.8
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    ChangeStreamPublisher<TResult> batchSize(int batchSize);

    /**
     * Helper to return a publisher limited to the first result.
     *
     * @return a Publisher which will contain a single item.
     * @since 1.8
     */
    Publisher<ChangeStreamDocument<TResult>> first();
}
