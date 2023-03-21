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

import com.mongodb.CursorType
import com.mongodb.ExplainVerbosity
import com.mongodb.client.FindIterable as JFindIterable
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

class FindIterableTest {
    @Test
    fun shouldHaveTheSameMethods() {
        val jFindIterableFunctions = JFindIterable::class.declaredFunctions.map { it.name }.toSet()
        val kFindIterableFunctions = FindIterable::class.declaredFunctions.map { it.name }.toSet()

        assertEquals(jFindIterableFunctions, kFindIterableFunctions)
    }

    @Suppress("DEPRECATION")
    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: JFindIterable<Document> = mock()
        val iterable = FindIterable(wrapped)

        val batchSize = 10
        val bson = BsonDocument()
        val bsonComment = BsonString("a comment")
        val collation = Collation.builder().locale("en").build()
        val comment = "comment"
        val filter = BsonDocument()
        val hint = Document("h", 1)
        val hintString = "hintString"
        val verbosity = ExplainVerbosity.QUERY_PLANNER

        whenever(wrapped.explain(Document::class.java)).doReturn(mock())
        whenever(wrapped.explain(Document::class.java, verbosity)).doReturn(mock())
        whenever(wrapped.explain(BsonDocument::class.java, verbosity)).doReturn(mock())

        iterable.allowDiskUse(true)
        iterable.batchSize(batchSize)
        iterable.collation(collation)
        iterable.comment(bsonComment)
        iterable.comment(comment)
        iterable.cursorType(CursorType.NonTailable)
        iterable.explain()
        iterable.explain(verbosity)
        iterable.explain(Document::class.java)
        iterable.explain(BsonDocument::class.java, verbosity)
        iterable.explain<Document>()
        iterable.explain<BsonDocument>(verbosity)
        iterable.filter(filter)
        iterable.hint(hint)
        iterable.hintString(hintString)
        iterable.let(bson)
        iterable.limit(1)
        iterable.max(bson)
        iterable.maxAwaitTime(1)
        iterable.maxAwaitTime(1, TimeUnit.SECONDS)
        iterable.maxTime(1)
        iterable.maxTime(1, TimeUnit.SECONDS)
        iterable.min(bson)
        iterable.oplogReplay(true)
        iterable.noCursorTimeout(true)
        iterable.partial(true)
        iterable.projection(bson)
        iterable.returnKey(true)
        iterable.showRecordId(true)
        iterable.skip(1)
        iterable.sort(bson)

        verify(wrapped).allowDiskUse(true)
        verify(wrapped).batchSize(batchSize)
        verify(wrapped).collation(collation)
        verify(wrapped).comment(bsonComment)
        verify(wrapped).comment(comment)
        verify(wrapped).cursorType(CursorType.NonTailable)
        verify(wrapped, times(3)).explain(Document::class.java)
        verify(wrapped, times(1)).explain(Document::class.java, verbosity)
        verify(wrapped, times(2)).explain(BsonDocument::class.java, verbosity)
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
        verify(wrapped).oplogReplay(true)
        verify(wrapped).noCursorTimeout(true)
        verify(wrapped).partial(true)
        verify(wrapped).projection(bson)
        verify(wrapped).returnKey(true)
        verify(wrapped).showRecordId(true)
        verify(wrapped).skip(1)
        verify(wrapped).sort(bson)

        verifyNoMoreInteractions(wrapped)
    }
}
