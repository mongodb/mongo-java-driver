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

import com.mongodb.ExplainVerbosity
import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.client.model.Collation
import com.mongodb.reactivestreams.client.AggregatePublisher
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredFunctions
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import reactor.core.publisher.Mono

class AggregateFlowTest {

    @Test
    fun shouldHaveTheSameMethods() {
        val jAggregatePublisherFunctions =
            AggregatePublisher::class.declaredFunctions.map { it.name }.toSet() - "first" - "subscribe"
        val kAggregateFlowFunctions = AggregateFlow::class.declaredFunctions.map { it.name }.toSet() - "collect"

        assertEquals(jAggregatePublisherFunctions, kAggregateFlowFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: AggregatePublisher<Document> = mock()
        val flow = AggregateFlow(wrapped)

        val batchSize = 10
        val bson = BsonDocument()
        val bsonComment = BsonString("a comment")
        val collation = Collation.builder().locale("en").build()
        val comment = "comment"
        val hint = Document("h", 1)
        val hintString = "hintString"
        val verbosity = ExplainVerbosity.QUERY_PLANNER

        flow.allowDiskUse(true)
        flow.batchSize(batchSize)
        flow.bypassDocumentValidation(true)
        flow.collation(collation)
        flow.comment(bsonComment)
        flow.comment(comment)
        flow.hint(hint)
        flow.hintString(hintString)
        flow.let(bson)
        flow.maxAwaitTime(1)
        flow.maxAwaitTime(1, TimeUnit.SECONDS)
        flow.maxTime(1)
        flow.maxTime(1, TimeUnit.SECONDS)
        flow.timeoutMode(TimeoutMode.ITERATION)

        verify(wrapped).allowDiskUse(true)
        verify(wrapped).batchSize(batchSize)
        verify(wrapped).bypassDocumentValidation(true)
        verify(wrapped).collation(collation)
        verify(wrapped).comment(bsonComment)
        verify(wrapped).comment(comment)
        verify(wrapped).hint(hint)
        verify(wrapped).hintString(hintString)
        verify(wrapped).maxAwaitTime(1, TimeUnit.MILLISECONDS)
        verify(wrapped).maxAwaitTime(1, TimeUnit.SECONDS)
        verify(wrapped).maxTime(1, TimeUnit.MILLISECONDS)
        verify(wrapped).maxTime(1, TimeUnit.SECONDS)
        verify(wrapped).let(bson)
        verify(wrapped).timeoutMode(TimeoutMode.ITERATION)

        whenever(wrapped.explain(Document::class.java)).doReturn(Mono.fromCallable { Document() })
        whenever(wrapped.explain(Document::class.java, verbosity)).doReturn(Mono.fromCallable { Document() })
        whenever(wrapped.explain(BsonDocument::class.java, verbosity)).doReturn(Mono.fromCallable { BsonDocument() })
        whenever(wrapped.toCollection()).doReturn(Mono.empty())

        runBlocking {
            flow.explain()
            flow.explain(verbosity)
            flow.explain(Document::class.java)
            flow.explain(BsonDocument::class.java, verbosity)
            flow.explain<Document>()
            flow.explain<BsonDocument>(verbosity)
            flow.toCollection()
        }

        verify(wrapped, times(3)).explain(Document::class.java)
        verify(wrapped, times(1)).explain(Document::class.java, verbosity)
        verify(wrapped, times(2)).explain(BsonDocument::class.java, verbosity)
        verify(wrapped).toCollection()

        verifyNoMoreInteractions(wrapped)
    }
}
