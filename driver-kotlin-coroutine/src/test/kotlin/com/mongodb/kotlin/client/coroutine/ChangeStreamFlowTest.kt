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
package com.mongodb.kotlin.client.coroutine

import com.mongodb.client.model.Collation
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.client.model.changestream.FullDocumentBeforeChange
import com.mongodb.reactivestreams.client.ChangeStreamPublisher
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredFunctions
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
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

class ChangeStreamFlowTest {

    @Test
    fun shouldHaveTheSameMethods() {
        val jChangeStreamPublisherFunctions =
            ChangeStreamPublisher::class.declaredFunctions.map { it.name }.toSet() - "first"
        val kChangeStreamFlowFunctions = ChangeStreamFlow::class.declaredFunctions.map { it.name }.toSet() - "collect"

        assertEquals(jChangeStreamPublisherFunctions, kChangeStreamFlowFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: ChangeStreamPublisher<Document> = mock()
        val flow = ChangeStreamFlow(wrapped)

        val batchSize = 10
        val bsonComment = BsonString("a comment")
        val collation = Collation.builder().locale("en").build()
        val comment = "comment"
        val operationTime = BsonTimestamp(1)
        val resumeToken = BsonDocument()

        flow.batchSize(batchSize)
        flow.collation(collation)
        flow.comment(comment)
        flow.comment(bsonComment)
        flow.fullDocument(FullDocument.UPDATE_LOOKUP)
        flow.fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED)
        flow.maxAwaitTime(1)
        flow.maxAwaitTime(1, TimeUnit.SECONDS)
        flow.resumeAfter(resumeToken)
        flow.showExpandedEvents(true)
        flow.startAfter(resumeToken)
        flow.startAtOperationTime(operationTime)

        verify(wrapped).batchSize(batchSize)
        verify(wrapped).collation(collation)
        verify(wrapped).comment(comment)
        verify(wrapped).comment(bsonComment)
        verify(wrapped).fullDocument(FullDocument.UPDATE_LOOKUP)
        verify(wrapped).fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED)
        verify(wrapped).maxAwaitTime(1, TimeUnit.MILLISECONDS)
        verify(wrapped).maxAwaitTime(1, TimeUnit.SECONDS)
        verify(wrapped).resumeAfter(resumeToken)
        verify(wrapped).showExpandedEvents(true)
        verify(wrapped).startAfter(resumeToken)
        verify(wrapped).startAtOperationTime(operationTime)

        whenever(wrapped.withDocumentClass(BsonDocument::class.java)).doReturn(mock())
        runBlocking { flow.withDocumentClass<BsonDocument>() }
        verify(wrapped).withDocumentClass(BsonDocument::class.java)

        verifyNoMoreInteractions(wrapped)
    }
}
