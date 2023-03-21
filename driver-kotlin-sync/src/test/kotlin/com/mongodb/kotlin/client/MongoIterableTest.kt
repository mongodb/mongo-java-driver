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

import com.mongodb.Function
import com.mongodb.client.MongoCursor as JMongoCursor
import com.mongodb.client.MongoIterable as JMongoIterable
import kotlin.test.assertContentEquals
import org.bson.Document
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever

class MongoIterableTest {

    @Suppress("UNCHECKED_CAST")
    @Test
    fun shouldCallTheUnderlyingMethods() {
        val delegate: JMongoIterable<Document> = mock()
        val cursor: JMongoCursor<Document> = mock()
        val iterable = MongoIterable(delegate)

        val batchSize = 10
        val documents = listOf(Document("a", 1), Document("b", 2), Document("c", 3))
        val transform: (Document) -> String = { it.toJson() }
        val transformClass: Class<Function<Document, String>> =
            Function::class.java as Class<Function<Document, String>>

        whenever(cursor.hasNext()).thenReturn(true, true, true, false, true, true, true, false, true, true, true, false)
        whenever(cursor.next())
            .thenReturn(
                documents[0],
                documents[1],
                documents[2],
                documents[0],
                documents[1],
                documents[2],
                documents[0],
                documents[1],
                documents[2])
        whenever(delegate.cursor()).doReturn(cursor)
        whenever(delegate.first()).doReturn(documents[0])

        whenever(delegate.map(ArgumentMatchers.any(transformClass))).doReturn(mock())

        iterable.batchSize(batchSize)
        iterable.cursor()
        iterable.first()
        iterable.firstOrNull()
        iterable.forEach { it.toString() }
        iterable.toCollection(mutableListOf())
        iterable.use { it.take(2) }
        iterable.map(transform)

        verify(delegate, times(1)).batchSize(batchSize)
        verify(delegate, times(4)).cursor()
        verify(delegate, times(2)).first()
        verify(delegate, times(1)).map(ArgumentMatchers.any(transformClass))

        verifyNoMoreInteractions(delegate)
    }

    @Test
    fun shouldCloseTheUnderlyingCursorWhenUsingUse() {
        val delegate: JMongoIterable<Document> = mock()
        val cursor: JMongoCursor<Document> = mock()
        val iterable = MongoIterable(delegate)

        val documents = listOf(Document("a", 1), Document("b", 2), Document("c", 3))

        whenever(cursor.hasNext()).thenReturn(true, true, true, false)
        whenever(cursor.next()).thenReturn(documents[0], documents[1], documents[2])
        whenever(delegate.cursor()).doReturn(cursor)

        assertContentEquals(documents.subList(0, 2), iterable.use { it.take(2) }.toList())

        verify(delegate, times(1)).cursor()
        verify(cursor, times(2)).hasNext()
        verify(cursor, times(2)).next()
        verify(cursor, times(1)).close()

        verifyNoMoreInteractions(delegate)
        verifyNoMoreInteractions(cursor)
    }

    @Test
    fun shouldCloseTheUnderlyingCursorWhenUsingToList() {
        val delegate: JMongoIterable<Document> = mock()
        val cursor: JMongoCursor<Document> = mock()
        val iterable = MongoIterable(delegate)

        val documents = listOf(Document("a", 1), Document("b", 2), Document("c", 3))

        whenever(cursor.hasNext()).thenReturn(true, true, true, false)
        whenever(cursor.next()).thenReturn(documents[0], documents[1], documents[2])
        whenever(delegate.cursor()).doReturn(cursor)

        assertContentEquals(documents, iterable.toList())

        verify(delegate, times(1)).cursor()
        verify(cursor, times(4)).hasNext()
        verify(cursor, times(3)).next()
        verify(cursor, times(1)).close()

        verifyNoMoreInteractions(delegate)
        verifyNoMoreInteractions(cursor)
    }
}
