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
package com.mongodb.kotlin.client

import com.mongodb.client.ChangeStreamIterable as JChangeStreamIterable
import com.mongodb.client.model.Collation
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.client.model.changestream.FullDocumentBeforeChange
import java.util.concurrent.TimeUnit
import org.bson.BsonDocument
import org.bson.BsonTimestamp
import org.bson.BsonValue

/**
 * Iterable like implementation for change streams.
 *
 * Note: the [ChangeStreamDocument] class will not be applicable for all change stream outputs. If using custom
 * pipelines that radically change the result, then the [withDocumentClass] method can be used to provide an alternative
 * document format.
 *
 * @param <T> The type of the result.
 */
public class ChangeStreamIterable<T>(@PublishedApi internal val wrapped: JChangeStreamIterable<T>) :
    MongoIterable<ChangeStreamDocument<T>>(wrapped) {

    /**
     * Returns a cursor used for iterating over elements of type {@code ChangeStreamDocument<TResult>}. The cursor has a
     * covariant return type to additionally provide a method to access the resume token in change stream batches.
     *
     * @return the change stream cursor
     */
    public override fun cursor(): MongoChangeStreamCursor<ChangeStreamDocument<T>> =
        MongoChangeStreamCursor(wrapped.cursor())

    /**
     * Sets the fullDocument value.
     *
     * @param fullDocument the fullDocument
     * @return this
     */
    public fun fullDocument(fullDocument: FullDocument): ChangeStreamIterable<T> = apply {
        wrapped.fullDocument(fullDocument)
    }

    /**
     * Sets the fullDocumentBeforeChange value.
     *
     * @param fullDocumentBeforeChange the fullDocumentBeforeChange
     * @return this
     */
    public fun fullDocumentBeforeChange(fullDocumentBeforeChange: FullDocumentBeforeChange): ChangeStreamIterable<T> =
        apply {
            wrapped.fullDocumentBeforeChange(fullDocumentBeforeChange)
        }

    /**
     * Sets the logical starting point for the new change stream.
     *
     * @param resumeToken the resume token
     * @return this
     */
    public fun resumeAfter(resumeToken: BsonDocument): ChangeStreamIterable<T> = apply {
        wrapped.resumeAfter(resumeToken)
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public override fun batchSize(batchSize: Int): ChangeStreamIterable<T> = apply { wrapped.batchSize(batchSize) }

    /**
     * Sets the maximum await execution time on the server for this operation.
     *
     * @param maxAwaitTime the max await time. A zero value will be ignored, and indicates that the driver should
     *   respect the server's default value
     * @param timeUnit the time unit, which defaults to MILLISECONDS
     * @return this
     */
    public fun maxAwaitTime(maxAwaitTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): ChangeStreamIterable<T> =
        apply {
            wrapped.maxAwaitTime(maxAwaitTime, timeUnit)
        }

    /**
     * Sets the collation options
     *
     * A null value represents the server default.
     *
     * @param collation the collation options to use
     * @return this
     */
    public fun collation(collation: Collation?): ChangeStreamIterable<T> = apply { wrapped.collation(collation) }

    /**
     * Returns a `MongoIterable` containing the results of the change stream based on the document class provided.
     *
     * @param R the Mongo Iterable type
     * @param resultClass the target document type of the iterable.
     * @return the new Mongo Iterable
     */
    public fun <R> withDocumentClass(resultClass: Class<R>): MongoIterable<R> =
        MongoIterable(wrapped.withDocumentClass(resultClass))

    /**
     * Returns a `MongoIterable` containing the results of the change stream based on the document class provided.
     *
     * @param R the Mongo Iterable type
     * @return the new Mongo Iterable
     */
    public inline fun <reified R : Any> withDocumentClass(): MongoIterable<R> = withDocumentClass(R::class.java)

    /**
     * The change stream will only provide changes that occurred at or after the specified timestamp.
     *
     * Any command run against the server will return an operation time that can be used here.
     *
     * The default value is an operation time obtained from the server before the change stream was created.
     *
     * @param startAtOperationTime the start at operation time
     * @return this
     */
    public fun startAtOperationTime(startAtOperationTime: BsonTimestamp): ChangeStreamIterable<T> = apply {
        wrapped.startAtOperationTime(startAtOperationTime)
    }

    /**
     * Similar to `resumeAfter`, this option takes a resume token and starts a new change stream returning the first
     * notification after the token.
     *
     * This will allow users to watch collections that have been dropped and recreated or newly renamed collections
     * without missing any notifications.
     *
     * Note: The server will report an error if both `startAfter` and `resumeAfter` are specified.
     *
     * @param startAfter the startAfter resumeToken
     * @return this
     * @see [Start After](https://www.mongodb.com/docs/manual/changeStreams/#change-stream-start-after)
     */
    public fun startAfter(startAfter: BsonDocument): ChangeStreamIterable<T> = apply { wrapped.startAfter(startAfter) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     */
    public fun comment(comment: String?): ChangeStreamIterable<T> = apply { wrapped.comment(comment) }
    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * The comment can be any valid BSON type for server versions 4.4 and above. Server versions between 3.6 and 4.2
     * only support string as comment, and providing a non-string type will result in a server-side error.
     *
     * @param comment the comment
     */
    public fun comment(comment: BsonValue?): ChangeStreamIterable<T> = apply { wrapped.comment(comment) }

    /**
     * Sets whether to include expanded change stream events, which are: createIndexes, dropIndexes, modify, create,
     * shardCollection, reshardCollection, refineCollectionShardKey. False by default.
     *
     * @param showExpandedEvents true to include expanded events
     * @return this
     */
    public fun showExpandedEvents(showExpandedEvents: Boolean): ChangeStreamIterable<T> = apply {
        wrapped.showExpandedEvents(showExpandedEvents)
    }
}
