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
package com.mongodb.kotlin.client.syncadapter

import com.mongodb.client.ChangeStreamIterable as JChangeStreamIterable
import com.mongodb.client.MongoIterable
import com.mongodb.client.model.Collation
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.client.model.changestream.FullDocumentBeforeChange
import com.mongodb.kotlin.client.ChangeStreamIterable
import java.util.concurrent.TimeUnit
import org.bson.BsonDocument
import org.bson.BsonTimestamp
import org.bson.BsonValue

data class SyncChangeStreamIterable<T : Any>(val wrapped: ChangeStreamIterable<T>) :
    JChangeStreamIterable<T>, SyncMongoIterable<ChangeStreamDocument<T>>(wrapped) {
    override fun <R : Any> withDocumentClass(clazz: Class<R>): MongoIterable<R> =
        SyncMongoIterable(wrapped.withDocumentClass(clazz))
    override fun batchSize(batchSize: Int): SyncChangeStreamIterable<T> = apply { wrapped.batchSize(batchSize) }
    override fun collation(collation: Collation?): SyncChangeStreamIterable<T> = apply { wrapped.collation(collation) }
    override fun comment(comment: BsonValue?): SyncChangeStreamIterable<T> = apply { wrapped.comment(comment) }
    override fun comment(comment: String?): SyncChangeStreamIterable<T> = apply { wrapped.comment(comment) }
    override fun cursor(): SyncMongoChangeStreamCursor<ChangeStreamDocument<T>> =
        SyncMongoChangeStreamCursor(wrapped.cursor())
    override fun fullDocument(fullDocument: FullDocument): SyncChangeStreamIterable<T> = apply {
        wrapped.fullDocument(fullDocument)
    }
    override fun fullDocumentBeforeChange(
        fullDocumentBeforeChange: FullDocumentBeforeChange
    ): SyncChangeStreamIterable<T> = apply { wrapped.fullDocumentBeforeChange(fullDocumentBeforeChange) }
    override fun maxAwaitTime(maxAwaitTime: Long, timeUnit: TimeUnit): SyncChangeStreamIterable<T> = apply {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit)
    }
    override fun resumeAfter(resumeToken: BsonDocument): SyncChangeStreamIterable<T> = apply {
        wrapped.resumeAfter(resumeToken)
    }
    override fun showExpandedEvents(showExpandedEvents: Boolean): SyncChangeStreamIterable<T> = apply {
        wrapped.showExpandedEvents(showExpandedEvents)
    }
    override fun startAfter(startAfter: BsonDocument): SyncChangeStreamIterable<T> = apply {
        wrapped.startAfter(startAfter)
    }
    override fun startAtOperationTime(startAtOperationTime: BsonTimestamp): SyncChangeStreamIterable<T> = apply {
        wrapped.startAtOperationTime(startAtOperationTime)
    }
}
