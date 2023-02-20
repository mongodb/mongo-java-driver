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

import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.ClientSession
import com.mongodb.client.MongoDatabase as JMongoDatabase
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.CreateViewOptions
import com.mongodb.client.model.ValidationAction
import com.mongodb.client.model.ValidationOptions
import com.mongodb.kotlin.client.MockitoHelper.deepRefEq
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.full.declaredMemberProperties
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.refEq
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever

class MongoDatabaseTest {

    @Mock val wrapped: JMongoDatabase = mock()
    @Mock val clientSession: ClientSession = mock()

    @Test
    fun shouldHaveTheSameMethods() {
        val jMongoDatabaseFunctions = JMongoDatabase::class.declaredFunctions.map { it.name }.toSet()
        val kMongoDatabaseFunctions =
            MongoDatabase::class.declaredFunctions.map { it.name }.toSet() +
                MongoDatabase::class
                    .declaredMemberProperties
                    .filterNot { it.name == "wrapped" }
                    .map { "get${it.name.replaceFirstChar{c -> c.uppercaseChar() }}" }

        assertEquals(jMongoDatabaseFunctions, kMongoDatabaseFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingGetNamespace() {
        val mongoDatabase = MongoDatabase(wrapped)
        whenever(wrapped.name).doReturn("name")

        mongoDatabase.name
        verify(wrapped).name
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetCodecRegistry() {
        val mongoDatabase = MongoDatabase(wrapped)
        whenever(wrapped.codecRegistry).doReturn(mock())

        mongoDatabase.codecRegistry
        verify(wrapped).codecRegistry
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetReadPreference() {
        val mongoDatabase = MongoDatabase(wrapped)
        whenever(wrapped.readPreference).doReturn(mock())

        mongoDatabase.readPreference
        verify(wrapped).readPreference
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetReadConcern() {
        val mongoDatabase = MongoDatabase(wrapped)
        whenever(wrapped.readConcern).doReturn(ReadConcern.DEFAULT)

        mongoDatabase.readConcern
        verify(wrapped).readConcern
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetWriteConcern() {
        val mongoDatabase = MongoDatabase(wrapped)
        whenever(wrapped.writeConcern).doReturn(mock())

        mongoDatabase.writeConcern
        verify(wrapped).writeConcern
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWithCodecRegistry() {
        val mongoDatabase = MongoDatabase(wrapped)
        val codecRegistry = mock<CodecRegistry>()
        whenever(wrapped.withCodecRegistry(codecRegistry)).doReturn(mock())

        mongoDatabase.withCodecRegistry(codecRegistry)
        verify(wrapped).withCodecRegistry(codecRegistry)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWithReadPreference() {
        val mongoDatabase = MongoDatabase(wrapped)
        val readPreference = ReadPreference.primaryPreferred()
        whenever(wrapped.withReadPreference(readPreference)).doReturn(mock())

        mongoDatabase.withReadPreference(readPreference)
        verify(wrapped).withReadPreference(readPreference)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWithReadConcern() {
        val mongoDatabase = MongoDatabase(wrapped)
        val readConcern = ReadConcern.AVAILABLE
        whenever(wrapped.withReadConcern(readConcern)).doReturn(mock())

        mongoDatabase.withReadConcern(readConcern)
        verify(wrapped).withReadConcern(readConcern)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWithWriteConcern() {
        val mongoDatabase = MongoDatabase(wrapped)
        val writeConcern = WriteConcern.MAJORITY
        whenever(wrapped.withWriteConcern(writeConcern)).doReturn(mock())

        mongoDatabase.withWriteConcern(writeConcern)
        verify(wrapped).withWriteConcern(writeConcern)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetCollection() {
        val mongoDatabase = MongoDatabase(wrapped)
        whenever(wrapped.getCollection("collectionName", Document::class.java)).doReturn(mock())

        mongoDatabase.getCollection<Document>("collectionName")
        verify(wrapped).getCollection("collectionName", Document::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingRunCommand() {
        val mongoDatabase = MongoDatabase(wrapped)
        val command = Document(mapOf("a" to 1))
        val primary = ReadPreference.primary()
        val primaryPreferred = ReadPreference.primaryPreferred()

        whenever(wrapped.readPreference).doReturn(primary)
        whenever(wrapped.runCommand(command, primary, Document::class.java)).doReturn(mock())
        whenever(wrapped.runCommand(clientSession, command, primary, Document::class.java)).doReturn(mock())
        whenever(wrapped.runCommand(command, primary, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.runCommand(clientSession, command, primary, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.runCommand(command, primaryPreferred, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.runCommand(clientSession, command, primaryPreferred, BsonDocument::class.java))
            .doReturn(mock())

        mongoDatabase.runCommand(command)
        mongoDatabase.runCommand(command, primary)
        mongoDatabase.runCommand(command, resultClass = Document::class.java)
        mongoDatabase.runCommand(command, primary, Document::class.java)

        mongoDatabase.runCommand(clientSession, command)
        mongoDatabase.runCommand(clientSession, command, primary)
        mongoDatabase.runCommand(clientSession, command, resultClass = Document::class.java)
        mongoDatabase.runCommand(clientSession, command, primary, Document::class.java)

        mongoDatabase.runCommand<BsonDocument>(command)
        mongoDatabase.runCommand<BsonDocument>(command, primaryPreferred)
        mongoDatabase.runCommand<BsonDocument>(clientSession, command)
        mongoDatabase.runCommand<BsonDocument>(clientSession, command, primaryPreferred)

        verify(wrapped, times(6)).readPreference
        verify(wrapped, times(4)).runCommand(command, primary, Document::class.java)
        verify(wrapped, times(4)).runCommand(clientSession, command, primary, Document::class.java)
        verify(wrapped, times(1)).runCommand(command, primary, BsonDocument::class.java)
        verify(wrapped, times(1)).runCommand(clientSession, command, primary, BsonDocument::class.java)
        verify(wrapped, times(1)).runCommand(command, primaryPreferred, BsonDocument::class.java)
        verify(wrapped, times(1)).runCommand(clientSession, command, primaryPreferred, BsonDocument::class.java)

        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingDrop() {
        val mongoDatabase = MongoDatabase(wrapped)

        mongoDatabase.drop()
        mongoDatabase.drop(clientSession)

        verify(wrapped).drop()
        verify(wrapped).drop(clientSession)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingListCollectionNames() {
        val mongoDatabase = MongoDatabase(wrapped)
        whenever(wrapped.listCollectionNames()).doReturn(mock())
        whenever(wrapped.listCollectionNames(clientSession)).doReturn(mock())

        mongoDatabase.listCollectionNames()
        mongoDatabase.listCollectionNames(clientSession)

        verify(wrapped).listCollectionNames()
        verify(wrapped).listCollectionNames(clientSession)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingListCollections() {
        val mongoDatabase = MongoDatabase(wrapped)
        whenever(wrapped.listCollections(Document::class.java)).doReturn(mock())
        whenever(wrapped.listCollections(BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.listCollections(clientSession, Document::class.java)).doReturn(mock())
        whenever(wrapped.listCollections(clientSession, BsonDocument::class.java)).doReturn(mock())

        mongoDatabase.listCollections()
        mongoDatabase.listCollections(clientSession)

        mongoDatabase.listCollections(resultClass = Document::class.java)
        mongoDatabase.listCollections(clientSession, Document::class.java)

        mongoDatabase.listCollections<BsonDocument>()
        mongoDatabase.listCollections<BsonDocument>(clientSession)

        verify(wrapped, times(2)).listCollections(Document::class.java)
        verify(wrapped, times(2)).listCollections(clientSession, Document::class.java)
        verify(wrapped, times(1)).listCollections(BsonDocument::class.java)
        verify(wrapped, times(1)).listCollections(clientSession, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingCreateCollection() {
        val mongoDatabase = MongoDatabase(wrapped)
        val name = "coll"
        val name2 = "coll2"
        val defaultOptions = CreateCollectionOptions()
        val options =
            CreateCollectionOptions().validationOptions(ValidationOptions().validationAction(ValidationAction.WARN))

        mongoDatabase.createCollection(name)
        mongoDatabase.createCollection(name2, options)
        mongoDatabase.createCollection(clientSession, name)
        mongoDatabase.createCollection(clientSession, name2, options)

        verify(wrapped).createCollection(eq(name), deepRefEq(defaultOptions))
        verify(wrapped).createCollection(eq(name2), eq(options))
        verify(wrapped).createCollection(eq(clientSession), eq(name), deepRefEq(defaultOptions))
        verify(wrapped).createCollection(eq(clientSession), eq(name2), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingCreateView() {
        val mongoDatabase = MongoDatabase(wrapped)
        val viewName = "view"
        val viewOn = "coll"
        val pipeline = listOf(Document(mapOf("a" to 1)))
        val defaultOptions = CreateViewOptions()
        val options = CreateViewOptions().collation(Collation.builder().backwards(true).build())

        mongoDatabase.createView(viewName, viewOn, pipeline)
        mongoDatabase.createView(viewName, viewOn, pipeline, options)
        mongoDatabase.createView(clientSession, viewName, viewOn, pipeline)
        mongoDatabase.createView(clientSession, viewName, viewOn, pipeline, options)

        verify(wrapped).createView(eq(viewName), eq(viewOn), eq(pipeline), refEq(defaultOptions))
        verify(wrapped).createView(eq(viewName), eq(viewOn), eq(pipeline), eq(options))
        verify(wrapped).createView(eq(clientSession), eq(viewName), eq(viewOn), eq(pipeline), refEq(defaultOptions))
        verify(wrapped).createView(eq(clientSession), eq(viewName), eq(viewOn), eq(pipeline), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingAggregate() {
        val mongoDatabase = MongoDatabase(wrapped)
        val pipeline = listOf(Document(mapOf("a" to 1)))

        whenever(wrapped.aggregate(pipeline, Document::class.java)).doReturn(mock())
        whenever(wrapped.aggregate(clientSession, pipeline, Document::class.java)).doReturn(mock())
        whenever(wrapped.aggregate(pipeline, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.aggregate(clientSession, pipeline, BsonDocument::class.java)).doReturn(mock())

        mongoDatabase.aggregate(pipeline)
        mongoDatabase.aggregate(clientSession, pipeline)

        mongoDatabase.aggregate(pipeline, resultClass = Document::class.java)
        mongoDatabase.aggregate(clientSession, pipeline, Document::class.java)

        mongoDatabase.aggregate<BsonDocument>(pipeline)
        mongoDatabase.aggregate<BsonDocument>(clientSession, pipeline)

        verify(wrapped, times(2)).aggregate(pipeline, Document::class.java)
        verify(wrapped, times(2)).aggregate(clientSession, pipeline, Document::class.java)
        verify(wrapped, times(1)).aggregate(pipeline, BsonDocument::class.java)
        verify(wrapped, times(1)).aggregate(clientSession, pipeline, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWatch() {
        val mongoDatabase = MongoDatabase(wrapped)
        val pipeline = listOf(Document(mapOf("a" to 1)))

        whenever(wrapped.watch(emptyList(), Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(pipeline, Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession, emptyList(), Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession, pipeline, Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(emptyList(), BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.watch(pipeline, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession, emptyList(), BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession, pipeline, BsonDocument::class.java)).doReturn(mock())

        mongoDatabase.watch()
        mongoDatabase.watch(pipeline)
        mongoDatabase.watch(clientSession)
        mongoDatabase.watch(clientSession, pipeline)

        mongoDatabase.watch(resultClass = Document::class.java)
        mongoDatabase.watch(pipeline, Document::class.java)
        mongoDatabase.watch(clientSession, resultClass = Document::class.java)
        mongoDatabase.watch(clientSession, pipeline, Document::class.java)

        mongoDatabase.watch<BsonDocument>()
        mongoDatabase.watch<BsonDocument>(pipeline)
        mongoDatabase.watch<BsonDocument>(clientSession)
        mongoDatabase.watch<BsonDocument>(clientSession, pipeline)

        verify(wrapped, times(2)).watch(emptyList(), Document::class.java)
        verify(wrapped, times(2)).watch(pipeline, Document::class.java)
        verify(wrapped, times(2)).watch(clientSession, emptyList(), Document::class.java)
        verify(wrapped, times(2)).watch(clientSession, pipeline, Document::class.java)
        verify(wrapped, times(1)).watch(emptyList(), BsonDocument::class.java)
        verify(wrapped, times(1)).watch(pipeline, BsonDocument::class.java)
        verify(wrapped, times(1)).watch(clientSession, emptyList(), BsonDocument::class.java)
        verify(wrapped, times(1)).watch(clientSession, pipeline, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldProvideExtensionFunctionsForTimeBasedOptions() {
        val oneThousand = 1000L

        assertEquals(oneThousand, CreateCollectionOptions().expireAfter(oneThousand).getExpireAfter(TimeUnit.SECONDS))
    }
}
