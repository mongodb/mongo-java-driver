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

import com.mongodb.ClientSessionOptions
import com.mongodb.client.MongoClient as JMongoClient
import kotlin.reflect.full.declaredFunctions
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.Document
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.refEq
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever

class MongoClientTest {

    @Mock val wrapped: JMongoClient = mock()
    @Mock val clientSession: ClientSession = ClientSession(mock())

    @Test
    fun shouldHaveTheSameMethods() {
        val jMongoClientFunctions = JMongoClient::class.declaredFunctions.map { it.name }.toSet()
        val kMongoClientFunctions = MongoClient::class.declaredFunctions.map { it.name }.toSet()

        assertEquals(jMongoClientFunctions, kMongoClientFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingClose() {
        val mongoClient = MongoClient(wrapped)
        mongoClient.close()

        verify(wrapped).close()
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingClusterDescription() {
        val mongoClient = MongoClient(wrapped)
        whenever(wrapped.clusterDescription).doReturn(mock())

        mongoClient.getClusterDescription()

        verify(wrapped).clusterDescription
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetDatabase() {
        val mongoClient = MongoClient(wrapped)
        whenever(wrapped.getDatabase(any())).doReturn(mock())

        mongoClient.getDatabase("dbName")
        verify(wrapped).getDatabase("dbName")
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shoulCallTheUnderlyingStartSession() {
        val mongoClient = MongoClient(wrapped)
        val defaultOptions = ClientSessionOptions.builder().build()
        val options = ClientSessionOptions.builder().causallyConsistent(true).build()

        whenever(wrapped.startSession(refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.startSession(options)).doReturn(mock())

        mongoClient.startSession()
        mongoClient.startSession(options)

        verify(wrapped).startSession(refEq(defaultOptions))
        verify(wrapped).startSession(options)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingListDatabaseNames() {
        val mongoClient = MongoClient(wrapped)
        whenever(wrapped.listDatabaseNames()).doReturn(mock())
        whenever(wrapped.listDatabaseNames(any())).doReturn(mock())

        mongoClient.listDatabaseNames()
        mongoClient.listDatabaseNames(clientSession)

        verify(wrapped).listDatabaseNames()
        verify(wrapped).listDatabaseNames(clientSession.wrapped)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingListDatabases() {
        val mongoClient = MongoClient(wrapped)
        whenever(wrapped.listDatabases(Document::class.java)).doReturn(mock())
        whenever(wrapped.listDatabases(clientSession.wrapped, Document::class.java)).doReturn(mock())
        whenever(wrapped.listDatabases(clientSession.wrapped, BsonDocument::class.java)).doReturn(mock())

        mongoClient.listDatabases()
        mongoClient.listDatabases(clientSession)
        mongoClient.listDatabases(Document::class.java)
        mongoClient.listDatabases(clientSession, BsonDocument::class.java)
        mongoClient.listDatabases<Document>()
        mongoClient.listDatabases<BsonDocument>(clientSession)

        verify(wrapped, times(3)).listDatabases(Document::class.java)
        verify(wrapped, times(1)).listDatabases(clientSession.wrapped, Document::class.java)
        verify(wrapped, times(2)).listDatabases(clientSession.wrapped, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWatch() {
        val mongoClient = MongoClient(wrapped)
        val pipeline = listOf(Document(mapOf("a" to 1)))

        whenever(wrapped.watch(emptyList(), Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(pipeline, Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession.wrapped, emptyList(), Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession.wrapped, pipeline, Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(pipeline, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession.wrapped, emptyList(), Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession.wrapped, pipeline, BsonDocument::class.java)).doReturn(mock())

        mongoClient.watch()
        mongoClient.watch(pipeline)
        mongoClient.watch(clientSession)
        mongoClient.watch(clientSession, pipeline)

        mongoClient.watch(resultClass = Document::class.java)
        mongoClient.watch(pipeline, BsonDocument::class.java)
        mongoClient.watch(clientSession = clientSession, resultClass = Document::class.java)
        mongoClient.watch(clientSession, pipeline, BsonDocument::class.java)

        mongoClient.watch<Document>()
        mongoClient.watch<BsonDocument>(pipeline)
        mongoClient.watch<Document>(clientSession)
        mongoClient.watch<BsonDocument>(clientSession, pipeline)

        verify(wrapped, times(3)).watch(emptyList(), Document::class.java)
        verify(wrapped, times(1)).watch(pipeline, Document::class.java)
        verify(wrapped, times(3)).watch(clientSession.wrapped, emptyList(), Document::class.java)
        verify(wrapped, times(1)).watch(clientSession.wrapped, pipeline, Document::class.java)
        verify(wrapped, times(2)).watch(pipeline, BsonDocument::class.java)
        verify(wrapped, times(2)).watch(clientSession.wrapped, pipeline, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }
}
