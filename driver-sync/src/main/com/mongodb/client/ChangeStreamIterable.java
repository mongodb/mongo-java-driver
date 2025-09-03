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

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

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
     * Returns a cursor used for iterating over elements of type {@code ChangeStreamDocument<TResult>}. The cursor has
     * a covariant return type to additionally provide a method to access the resume token in change stream batches.
     *
     * @return the change stream cursor
     * @since 3.11
     */
    MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor();

    /**
     * Sets the fullDocument value.
     *
     * @param fullDocument the fullDocument
     * @return this
     */
    ChangeStreamIterable<TResult> fullDocument(FullDocument fullDocument);


    /**
     * Sets the fullDocumentBeforeChange value.
     *
     * @param fullDocumentBeforeChange the fullDocumentBeforeChange
     * @return this
     * @since 4.7
     * @mongodb.server.release 6.0
     */
    ChangeStreamIterable<TResult> fullDocumentBeforeChange(FullDocumentBeforeChange fullDocumentBeforeChange);

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
     * @since 3.11
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual changeStreams/#change-stream-start-after
     */
    ChangeStreamIterable<TResult> startAfter(BsonDocument startAfter);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 3.6
     */
    ChangeStreamIterable<TResult> comment(@Nullable String comment);

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
    ChangeStreamIterable<TResult> comment(@Nullable BsonValue comment);

    /**
     * Sets whether to include expanded change stream events, which are:
     * createIndexes, dropIndexes, modify, create, shardCollection,
     * reshardCollection, refineCollectionShardKey. False by default.
     *
     * @param showExpandedEvents true to include expanded events
     * @return this
     * @since 4.7
     * @mongodb.server.release 6.0
     */
    ChangeStreamIterable<TResult> showExpandedEvents(boolean showExpandedEvents);
}
