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
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.client.model.changestream.FullDocumentBeforeChange
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredFunctions
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.Document
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever

class ChangeStreamIterableTest {

    @Test
    fun shouldHaveTheSameMethods() {
        val jChangeStreamIterableFunctions = JChangeStreamIterable::class.declaredFunctions.map { it.name }.toSet()
        val kChangeStreamIterableFunctions = ChangeStreamIterable::class.declaredFunctions.map { it.name }.toSet()

        assertEquals(jChangeStreamIterableFunctions, kChangeStreamIterableFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: JChangeStreamIterable<Document> = mock()
        val iterable = ChangeStreamIterable(wrapped)

        val batchSize = 10
        val bsonComment = BsonString("a comment")
        val collation = Collation.builder().locale("en").build()
        val comment = "comment"
        val operationTime = BsonTimestamp(1)
        val resumeToken = BsonDocument()

        whenever(wrapped.withDocumentClass(BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.cursor()).doReturn(mock())

        iterable.batchSize(batchSize)
        iterable.collation(collation)
        iterable.comment(comment)
        iterable.comment(bsonComment)
        iterable.cursor()
        iterable.fullDocument(FullDocument.UPDATE_LOOKUP)
        iterable.fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED)
        iterable.maxAwaitTime(1)
        iterable.maxAwaitTime(1, TimeUnit.SECONDS)
        iterable.resumeAfter(resumeToken)
        iterable.showExpandedEvents(true)
        iterable.startAfter(resumeToken)
        iterable.startAtOperationTime(operationTime)
        iterable.withDocumentClass<BsonDocument>()

        verify(wrapped).batchSize(batchSize)
        verify(wrapped).collation(collation)
        verify(wrapped).comment(comment)
        verify(wrapped).comment(bsonComment)
        verify(wrapped).cursor()
        verify(wrapped).fullDocument(FullDocument.UPDATE_LOOKUP)
        verify(wrapped).fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED)
        verify(wrapped).maxAwaitTime(1, TimeUnit.MILLISECONDS)
        verify(wrapped).maxAwaitTime(1, TimeUnit.SECONDS)
        verify(wrapped).resumeAfter(resumeToken)
        verify(wrapped).showExpandedEvents(true)
        verify(wrapped).startAfter(resumeToken)
        verify(wrapped).startAtOperationTime(operationTime)
        verify(wrapped).withDocumentClass(BsonDocument::class.java)

        verifyNoMoreInteractions(wrapped)
    }
}
