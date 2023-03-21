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

import com.mongodb.ExplainVerbosity
import com.mongodb.client.AggregateIterable as JAggregateIterable
import com.mongodb.client.model.Collation
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredFunctions
import kotlin.test.assertEquals
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

class AggregateIterableTest {

    @Test
    fun shouldHaveTheSameMethods() {
        val jAggregateIterableFunctions = JAggregateIterable::class.declaredFunctions.map { it.name }.toSet()
        val kAggregateIterableFunctions = AggregateIterable::class.declaredFunctions.map { it.name }.toSet()

        assertEquals(jAggregateIterableFunctions, kAggregateIterableFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: JAggregateIterable<Document> = mock()
        val iterable = AggregateIterable(wrapped)

        val batchSize = 10
        val bson = BsonDocument()
        val bsonComment = BsonString("a comment")
        val collation = Collation.builder().locale("en").build()
        val comment = "comment"
        val hint = Document("h", 1)
        val hintString = "hintString"
        val verbosity = ExplainVerbosity.QUERY_PLANNER

        whenever(wrapped.explain(Document::class.java)).doReturn(mock())
        whenever(wrapped.explain(Document::class.java, verbosity)).doReturn(mock())
        whenever(wrapped.explain(BsonDocument::class.java, verbosity)).doReturn(mock())

        iterable.allowDiskUse(true)
        iterable.batchSize(batchSize)
        iterable.bypassDocumentValidation(true)
        iterable.collation(collation)
        iterable.comment(bsonComment)
        iterable.comment(comment)
        iterable.explain()
        iterable.explain(verbosity)
        iterable.explain(Document::class.java)
        iterable.explain(BsonDocument::class.java, verbosity)
        iterable.explain<Document>()
        iterable.explain<BsonDocument>(verbosity)
        iterable.hint(hint)
        iterable.hintString(hintString)
        iterable.let(bson)
        iterable.maxAwaitTime(1)
        iterable.maxAwaitTime(1, TimeUnit.SECONDS)
        iterable.maxTime(1)
        iterable.maxTime(1, TimeUnit.SECONDS)

        verify(wrapped).allowDiskUse(true)
        verify(wrapped).batchSize(batchSize)
        verify(wrapped).bypassDocumentValidation(true)
        verify(wrapped).collation(collation)
        verify(wrapped).comment(bsonComment)
        verify(wrapped).comment(comment)
        verify(wrapped, times(3)).explain(Document::class.java)
        verify(wrapped, times(1)).explain(Document::class.java, verbosity)
        verify(wrapped, times(2)).explain(BsonDocument::class.java, verbosity)
        verify(wrapped).hint(hint)
        verify(wrapped).hintString(hintString)
        verify(wrapped).maxAwaitTime(1, TimeUnit.MILLISECONDS)
        verify(wrapped).maxAwaitTime(1, TimeUnit.SECONDS)
        verify(wrapped).maxTime(1, TimeUnit.MILLISECONDS)
        verify(wrapped).maxTime(1, TimeUnit.SECONDS)
        verify(wrapped).let(bson)

        iterable.toCollection()
        verify(wrapped).toCollection()

        verifyNoMoreInteractions(wrapped)
    }
}
