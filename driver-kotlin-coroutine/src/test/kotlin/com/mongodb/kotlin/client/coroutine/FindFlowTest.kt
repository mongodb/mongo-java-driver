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

import com.mongodb.CursorType
import com.mongodb.ExplainVerbosity
import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.client.model.Collation
import com.mongodb.reactivestreams.client.FindPublisher
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredFunctions
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import reactor.core.publisher.Mono

class FindFlowTest {
    @Test
    fun shouldHaveTheSameMethods() {
        val jFindPublisherFunctions = FindPublisher::class.declaredFunctions.map { it.name }.toSet() - "first"
        val kFindFlowFunctions = FindFlow::class.declaredFunctions.map { it.name }.toSet() - "collect"

        assertEquals(jFindPublisherFunctions, kFindFlowFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: FindPublisher<Document> = mock()
        val flow = FindFlow(wrapped)

        val batchSize = 10
        val bson = BsonDocument()
        val bsonComment = BsonString("a comment")
        val collation = Collation.builder().locale("en").build()
        val comment = "comment"
        val filter = BsonDocument()
        val hint = Document("h", 1)
        val hintString = "hintString"
        val verbosity = ExplainVerbosity.QUERY_PLANNER

        flow.allowDiskUse(true)
        flow.batchSize(batchSize)
        flow.collation(collation)
        flow.comment(bsonComment)
        flow.comment(comment)
        flow.cursorType(CursorType.NonTailable)
        flow.filter(filter)
        flow.hint(hint)
        flow.hintString(hintString)
        flow.let(bson)
        flow.limit(1)
        flow.max(bson)
        flow.maxAwaitTime(1)
        flow.maxAwaitTime(1, TimeUnit.SECONDS)
        flow.maxTime(1)
        flow.maxTime(1, TimeUnit.SECONDS)
        flow.min(bson)
        flow.noCursorTimeout(true)
        flow.partial(true)
        flow.projection(bson)
        flow.returnKey(true)
        flow.showRecordId(true)
        flow.skip(1)
        flow.sort(bson)
        flow.timeoutMode(TimeoutMode.ITERATION)

        verify(wrapped).allowDiskUse(true)
        verify(wrapped).batchSize(batchSize)
        verify(wrapped).collation(collation)
        verify(wrapped).comment(bsonComment)
        verify(wrapped).comment(comment)
        verify(wrapped).cursorType(CursorType.NonTailable)
        verify(wrapped).filter(filter)
        verify(wrapped).hint(hint)
        verify(wrapped).hintString(hintString)
        verify(wrapped).let(bson)
        verify(wrapped).limit(1)
        verify(wrapped).max(bson)
        verify(wrapped).maxAwaitTime(1, TimeUnit.MILLISECONDS)
        verify(wrapped).maxAwaitTime(1, TimeUnit.SECONDS)
        verify(wrapped).maxTime(1, TimeUnit.MILLISECONDS)
        verify(wrapped).maxTime(1, TimeUnit.SECONDS)
        verify(wrapped).min(bson)
        verify(wrapped).noCursorTimeout(true)
        verify(wrapped).partial(true)
        verify(wrapped).projection(bson)
        verify(wrapped).returnKey(true)
        verify(wrapped).showRecordId(true)
        verify(wrapped).skip(1)
        verify(wrapped).sort(bson)
        verify(wrapped).timeoutMode(TimeoutMode.ITERATION)

        whenever(wrapped.explain(Document::class.java)).doReturn(Mono.fromCallable { Document() })
        whenever(wrapped.explain(Document::class.java, verbosity)).doReturn(Mono.fromCallable { Document() })
        whenever(wrapped.explain(BsonDocument::class.java, verbosity)).doReturn(Mono.fromCallable { BsonDocument() })

        runBlocking {
            flow.explain()
            flow.explain(verbosity)
            flow.explain(Document::class.java)
            flow.explain(BsonDocument::class.java, verbosity)
            flow.explain<Document>()
            flow.explain<BsonDocument>(verbosity)
        }

        verify(wrapped, times(3)).explain(Document::class.java)
        verify(wrapped, times(1)).explain(Document::class.java, verbosity)
        verify(wrapped, times(2)).explain(BsonDocument::class.java, verbosity)

        verifyNoMoreInteractions(wrapped)
    }
}
