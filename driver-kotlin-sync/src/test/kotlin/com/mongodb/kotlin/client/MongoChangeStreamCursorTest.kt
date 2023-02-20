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

import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.client.MongoChangeStreamCursor as JMongoChangeStreamCursor
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.full.declaredMemberProperties
import kotlin.test.assertEquals
import org.bson.Document
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever

class MongoChangeStreamCursorTest {
    @Test
    fun shouldHaveTheSameMethods() {
        val jMongoChangeStreamCursorFunctions =
            JMongoChangeStreamCursor::class.declaredFunctions.map { it.name }.toSet()
        val kMongoChangeStreamCursorFunctions =
            MongoChangeStreamCursor::class.declaredFunctions.map { it.name }.toSet() +
                MongoChangeStreamCursor::class
                    .declaredMemberProperties
                    .filterNot { it.name == "wrapped" }
                    .map { "get${it.name.replaceFirstChar{c -> c.uppercaseChar() }}" }

        assertEquals(jMongoChangeStreamCursorFunctions, kMongoChangeStreamCursorFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: JMongoChangeStreamCursor<Document> = mock()
        val cursor = MongoChangeStreamCursor(wrapped)

        whenever(wrapped.resumeToken).doReturn(mock())
        whenever(wrapped.serverCursor).doReturn(ServerCursor(1, ServerAddress()))
        whenever(wrapped.serverAddress).doReturn(mock())

        cursor.getServerCursor()
        cursor.serverAddress
        cursor.hasNext()
        cursor.tryNext()
        cursor.available()
        cursor.getResumeToken()

        verify(wrapped).serverCursor
        verify(wrapped).serverAddress
        verify(wrapped).hasNext()
        verify(wrapped).tryNext()
        verify(wrapped).available()
        verify(wrapped).resumeToken

        verifyNoMoreInteractions(wrapped)
    }
}
