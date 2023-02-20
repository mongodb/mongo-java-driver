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

import com.mongodb.CreateIndexCommitQuorum
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.MongoCollection as JMongoCollection
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.CountOptions
import com.mongodb.client.model.CreateIndexOptions
import com.mongodb.client.model.DeleteOptions
import com.mongodb.client.model.DropCollectionOptions
import com.mongodb.client.model.DropIndexOptions
import com.mongodb.client.model.EstimatedDocumentCountOptions
import com.mongodb.client.model.FindOneAndDeleteOptions
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.IndexModel
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.InsertOneOptions
import com.mongodb.client.model.RenameCollectionOptions
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.UpdateOptions
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.full.declaredMemberProperties
import kotlin.test.assertContentEquals
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

class MongoCollectionTest {

    @Mock val wrapped: JMongoCollection<Document> = mock()
    @Mock val clientSession: ClientSession = ClientSession(mock())

    private val defaultFilter = BsonDocument()
    private val filter = Document("a", 1)
    private val pipeline = listOf(Document(mapOf("a" to 1)))

    @Test
    fun shouldHaveTheSameMethods() {
        val jMongoCollectionFunctions = JMongoCollection::class.declaredFunctions.map { it.name }.toSet()
        val kMongoCollectionFunctions =
            MongoCollection::class.declaredFunctions.map { it.name }.toSet() +
                MongoCollection::class
                    .declaredMemberProperties
                    .filterNot { it.name == "wrapped" }
                    .map { "get${it.name.replaceFirstChar { c -> c.uppercaseChar() }}" }

        assertEquals(jMongoCollectionFunctions, kMongoCollectionFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingGetDocumentClass() {
        val mongoCollection = MongoCollection(wrapped)
        whenever(wrapped.documentClass).doReturn(Document::class.java)

        mongoCollection.documentClass
        verify(wrapped).documentClass
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetNamespace() {
        val mongoCollection = MongoCollection(wrapped)
        whenever(wrapped.namespace).doReturn(MongoNamespace("a.b"))

        mongoCollection.namespace
        verify(wrapped).namespace
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetCodecRegistry() {
        val mongoCollection = MongoCollection(wrapped)
        whenever(wrapped.codecRegistry).doReturn(mock())

        mongoCollection.codecRegistry
        verify(wrapped).codecRegistry
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetReadPreference() {
        val mongoCollection = MongoCollection(wrapped)
        whenever(wrapped.readPreference).doReturn(mock())

        mongoCollection.readPreference
        verify(wrapped).readPreference
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetReadConcern() {
        val mongoCollection = MongoCollection(wrapped)
        whenever(wrapped.readConcern).doReturn(ReadConcern.DEFAULT)

        mongoCollection.readConcern
        verify(wrapped).readConcern
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingGetWriteConcern() {
        val mongoCollection = MongoCollection(wrapped)
        whenever(wrapped.writeConcern).doReturn(mock())

        mongoCollection.writeConcern
        verify(wrapped).writeConcern
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWithDocumentClass() {
        val mongoCollection = MongoCollection(wrapped)
        whenever(wrapped.withDocumentClass(BsonDocument::class.java)).doReturn(mock())

        mongoCollection.withDocumentClass<BsonDocument>()
        verify(wrapped).withDocumentClass(BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWithCodecRegistry() {
        val mongoCollection = MongoCollection(wrapped)
        val codecRegistry = mock<CodecRegistry>()
        whenever(wrapped.withCodecRegistry(codecRegistry)).doReturn(mock())

        mongoCollection.withCodecRegistry(codecRegistry)
        verify(wrapped).withCodecRegistry(codecRegistry)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWithReadPreference() {
        val mongoCollection = MongoCollection(wrapped)
        val readPreference = ReadPreference.primaryPreferred()
        whenever(wrapped.withReadPreference(readPreference)).doReturn(mock())

        mongoCollection.withReadPreference(readPreference)
        verify(wrapped).withReadPreference(readPreference)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWithReadConcern() {
        val mongoCollection = MongoCollection(wrapped)
        val readConcern = ReadConcern.AVAILABLE
        whenever(wrapped.withReadConcern(readConcern)).doReturn(mock())

        mongoCollection.withReadConcern(readConcern)
        verify(wrapped).withReadConcern(readConcern)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWithWriteConcern() {
        val mongoCollection = MongoCollection(wrapped)
        val writeConcern = WriteConcern.MAJORITY
        whenever(wrapped.withWriteConcern(writeConcern)).doReturn(mock())

        mongoCollection.withWriteConcern(writeConcern)
        verify(wrapped).withWriteConcern(writeConcern)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingCountDocuments() {
        val mongoCollection = MongoCollection(wrapped)

        val defaultOptions = CountOptions()

        val options = CountOptions().comment("comment")

        whenever(wrapped.countDocuments(eq(defaultFilter), refEq(defaultOptions))).doReturn(1)
        whenever(wrapped.countDocuments(eq(filter), refEq(defaultOptions))).doReturn(2)
        whenever(wrapped.countDocuments(eq(filter), eq(options))).doReturn(3)
        whenever(wrapped.countDocuments(eq(clientSession.wrapped), eq(defaultFilter), refEq(defaultOptions)))
            .doReturn(4)
        whenever(wrapped.countDocuments(eq(clientSession.wrapped), eq(filter), refEq(defaultOptions))).doReturn(5)
        whenever(wrapped.countDocuments(eq(clientSession.wrapped), eq(filter), eq(options))).doReturn(6)

        assertEquals(1, mongoCollection.countDocuments())
        assertEquals(2, mongoCollection.countDocuments(filter))
        assertEquals(3, mongoCollection.countDocuments(filter, options))
        assertEquals(4, mongoCollection.countDocuments(clientSession))
        assertEquals(5, mongoCollection.countDocuments(clientSession, filter))
        assertEquals(6, mongoCollection.countDocuments(clientSession, filter, options))

        verify(wrapped).countDocuments(eq(defaultFilter), refEq(defaultOptions))
        verify(wrapped).countDocuments(eq(filter), refEq(defaultOptions))
        verify(wrapped).countDocuments(eq(filter), eq(options))
        verify(wrapped).countDocuments(eq(clientSession.wrapped), eq(defaultFilter), refEq(defaultOptions))
        verify(wrapped).countDocuments(eq(clientSession.wrapped), eq(filter), refEq(defaultOptions))
        verify(wrapped).countDocuments(eq(clientSession.wrapped), eq(filter), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingEstimatedDocumentCount() {
        val mongoCollection = MongoCollection(wrapped)
        val defaultOptions = EstimatedDocumentCountOptions()
        val options = EstimatedDocumentCountOptions().comment("comment")

        whenever(wrapped.estimatedDocumentCount(refEq(defaultOptions))).doReturn(1)
        whenever(wrapped.estimatedDocumentCount(options)).doReturn(2)

        assertEquals(1, mongoCollection.estimatedDocumentCount())
        assertEquals(2, mongoCollection.estimatedDocumentCount(options))

        verify(wrapped).estimatedDocumentCount(refEq(defaultOptions))
        verify(wrapped).estimatedDocumentCount(options)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingDistinct() {
        val mongoCollection = MongoCollection(wrapped)
        val fieldName = "fieldName"

        whenever(wrapped.distinct(fieldName, defaultFilter, Document::class.java)).doReturn(mock())
        whenever(wrapped.distinct(fieldName, filter, Document::class.java)).doReturn(mock())
        whenever(wrapped.distinct(clientSession.wrapped, fieldName, defaultFilter, Document::class.java))
            .doReturn(mock())
        whenever(wrapped.distinct(clientSession.wrapped, fieldName, filter, Document::class.java)).doReturn(mock())
        whenever(wrapped.distinct(fieldName, defaultFilter, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.distinct(fieldName, filter, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.distinct(clientSession.wrapped, fieldName, defaultFilter, BsonDocument::class.java))
            .doReturn(mock())
        whenever(wrapped.distinct(clientSession.wrapped, fieldName, filter, BsonDocument::class.java)).doReturn(mock())

        mongoCollection.distinct("fieldName", resultClass = Document::class.java)
        mongoCollection.distinct("fieldName", filter, Document::class.java)
        mongoCollection.distinct(clientSession, "fieldName", resultClass = Document::class.java)
        mongoCollection.distinct(clientSession, "fieldName", filter, Document::class.java)

        mongoCollection.distinct<BsonDocument>("fieldName")
        mongoCollection.distinct<BsonDocument>("fieldName", filter)
        mongoCollection.distinct<BsonDocument>(clientSession, "fieldName")
        mongoCollection.distinct<BsonDocument>(clientSession, "fieldName", filter)

        verify(wrapped).distinct(fieldName, defaultFilter, Document::class.java)
        verify(wrapped).distinct(fieldName, filter, Document::class.java)
        verify(wrapped).distinct(clientSession.wrapped, fieldName, defaultFilter, Document::class.java)
        verify(wrapped).distinct(clientSession.wrapped, fieldName, filter, Document::class.java)

        verify(wrapped).distinct(fieldName, defaultFilter, BsonDocument::class.java)
        verify(wrapped).distinct(fieldName, filter, BsonDocument::class.java)
        verify(wrapped).distinct(clientSession.wrapped, fieldName, defaultFilter, BsonDocument::class.java)
        verify(wrapped).distinct(clientSession.wrapped, fieldName, filter, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingFind() {
        val mongoCollection = MongoCollection(wrapped)

        whenever(wrapped.documentClass).doReturn(Document::class.java)
        whenever(wrapped.find(defaultFilter, Document::class.java)).doReturn(mock())
        whenever(wrapped.find(filter, Document::class.java)).doReturn(mock())
        whenever(wrapped.find(clientSession.wrapped, defaultFilter, Document::class.java)).doReturn(mock())
        whenever(wrapped.find(clientSession.wrapped, filter, Document::class.java)).doReturn(mock())
        whenever(wrapped.find(defaultFilter, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.find(filter, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.find(clientSession.wrapped, defaultFilter, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.find(clientSession.wrapped, filter, BsonDocument::class.java)).doReturn(mock())

        mongoCollection.find()
        mongoCollection.find(filter)
        mongoCollection.find(clientSession)
        mongoCollection.find(clientSession, filter)

        mongoCollection.find(resultClass = Document::class.java)
        mongoCollection.find(filter, resultClass = Document::class.java)
        mongoCollection.find(clientSession, resultClass = Document::class.java)
        mongoCollection.find(clientSession, filter, Document::class.java)

        mongoCollection.find<BsonDocument>()
        mongoCollection.find<BsonDocument>(filter)
        mongoCollection.find<BsonDocument>(clientSession)
        mongoCollection.find<BsonDocument>(clientSession, filter)

        verify(wrapped, times(4)).documentClass
        verify(wrapped, times(2)).find(defaultFilter, Document::class.java)
        verify(wrapped, times(2)).find(filter, Document::class.java)
        verify(wrapped, times(2)).find(clientSession.wrapped, defaultFilter, Document::class.java)
        verify(wrapped, times(2)).find(clientSession.wrapped, filter, Document::class.java)
        verify(wrapped, times(1)).find(defaultFilter, BsonDocument::class.java)
        verify(wrapped, times(1)).find(filter, BsonDocument::class.java)
        verify(wrapped, times(1)).find(clientSession.wrapped, defaultFilter, BsonDocument::class.java)
        verify(wrapped, times(1)).find(clientSession.wrapped, filter, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingAggregate() {
        val mongoCollection = MongoCollection(wrapped)

        whenever(wrapped.documentClass).doReturn(Document::class.java)
        whenever(wrapped.aggregate(pipeline, Document::class.java)).doReturn(mock())
        whenever(wrapped.aggregate(clientSession.wrapped, pipeline, Document::class.java)).doReturn(mock())
        whenever(wrapped.aggregate(pipeline, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.aggregate(clientSession.wrapped, pipeline, BsonDocument::class.java)).doReturn(mock())

        mongoCollection.aggregate(pipeline)
        mongoCollection.aggregate(clientSession, pipeline)

        mongoCollection.aggregate(pipeline, resultClass = Document::class.java)
        mongoCollection.aggregate(clientSession, pipeline, Document::class.java)

        mongoCollection.aggregate<BsonDocument>(pipeline)
        mongoCollection.aggregate<BsonDocument>(clientSession, pipeline)

        verify(wrapped, times(2)).documentClass
        verify(wrapped, times(2)).aggregate(pipeline, Document::class.java)
        verify(wrapped, times(2)).aggregate(clientSession.wrapped, pipeline, Document::class.java)
        verify(wrapped, times(1)).aggregate(pipeline, BsonDocument::class.java)
        verify(wrapped, times(1)).aggregate(clientSession.wrapped, pipeline, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingWatch() {
        val mongoCollection = MongoCollection(wrapped)
        val pipeline = listOf(Document(mapOf("a" to 1)))

        whenever(wrapped.documentClass).doReturn(Document::class.java)
        whenever(wrapped.watch(emptyList(), Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(pipeline, Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession.wrapped, emptyList(), Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession.wrapped, pipeline, Document::class.java)).doReturn(mock())
        whenever(wrapped.watch(emptyList(), BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.watch(pipeline, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession.wrapped, emptyList(), BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.watch(clientSession.wrapped, pipeline, BsonDocument::class.java)).doReturn(mock())

        mongoCollection.watch()
        mongoCollection.watch(pipeline)
        mongoCollection.watch(clientSession)
        mongoCollection.watch(clientSession, pipeline)

        mongoCollection.watch(resultClass = Document::class.java)
        mongoCollection.watch(pipeline, Document::class.java)
        mongoCollection.watch(clientSession, resultClass = Document::class.java)
        mongoCollection.watch(clientSession, pipeline, Document::class.java)

        mongoCollection.watch<BsonDocument>()
        mongoCollection.watch<BsonDocument>(pipeline)
        mongoCollection.watch<BsonDocument>(clientSession)
        mongoCollection.watch<BsonDocument>(clientSession, pipeline)

        verify(wrapped, times(4)).documentClass
        verify(wrapped, times(2)).watch(emptyList(), Document::class.java)
        verify(wrapped, times(2)).watch(pipeline, Document::class.java)
        verify(wrapped, times(2)).watch(clientSession.wrapped, emptyList(), Document::class.java)
        verify(wrapped, times(2)).watch(clientSession.wrapped, pipeline, Document::class.java)
        verify(wrapped, times(1)).watch(emptyList(), BsonDocument::class.java)
        verify(wrapped, times(1)).watch(pipeline, BsonDocument::class.java)
        verify(wrapped, times(1)).watch(clientSession.wrapped, emptyList(), BsonDocument::class.java)
        verify(wrapped, times(1)).watch(clientSession.wrapped, pipeline, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Suppress("DEPRECATION")
    @Test
    fun shouldCallTheUnderlyingMapReduce() {
        val mongoCollection = MongoCollection(wrapped)
        val mapFunction = "mapper"
        val reduceFunction = "mapper"

        whenever(wrapped.documentClass).doReturn(Document::class.java)
        whenever(wrapped.mapReduce(mapFunction, reduceFunction, Document::class.java)).doReturn(mock())
        whenever(wrapped.mapReduce(clientSession.wrapped, mapFunction, reduceFunction, Document::class.java))
            .doReturn(mock())
        whenever(wrapped.mapReduce(mapFunction, reduceFunction, BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.mapReduce(clientSession.wrapped, mapFunction, reduceFunction, BsonDocument::class.java))
            .doReturn(mock())

        mongoCollection.mapReduce(mapFunction, reduceFunction)
        mongoCollection.mapReduce(clientSession, mapFunction, reduceFunction)

        mongoCollection.mapReduce(mapFunction, reduceFunction, Document::class.java)
        mongoCollection.mapReduce(clientSession, mapFunction, reduceFunction, Document::class.java)

        mongoCollection.mapReduce<BsonDocument>(mapFunction, reduceFunction)
        mongoCollection.mapReduce<BsonDocument>(clientSession, mapFunction, reduceFunction)

        verify(wrapped, times(2)).documentClass
        verify(wrapped, times(2)).mapReduce(mapFunction, reduceFunction, Document::class.java)
        verify(wrapped, times(2)).mapReduce(clientSession.wrapped, mapFunction, reduceFunction, Document::class.java)
        verify(wrapped, times(1)).mapReduce(mapFunction, reduceFunction, BsonDocument::class.java)
        verify(wrapped, times(1))
            .mapReduce(clientSession.wrapped, mapFunction, reduceFunction, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingInsertOne() {
        val mongoCollection = MongoCollection(wrapped)
        val value = Document("u", 1)
        val defaultOptions = InsertOneOptions()
        val options = InsertOneOptions().comment("comment")

        whenever(wrapped.insertOne(eq(value), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.insertOne(eq(value), eq(options))).doReturn(mock())
        whenever(wrapped.insertOne(eq(clientSession.wrapped), eq(value), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.insertOne(eq(clientSession.wrapped), eq(value), eq(options))).doReturn(mock())

        mongoCollection.insertOne(value)
        mongoCollection.insertOne(value, options)
        mongoCollection.insertOne(clientSession, value)
        mongoCollection.insertOne(clientSession, value, options)

        verify(wrapped).insertOne(eq(value), refEq(defaultOptions))
        verify(wrapped).insertOne(eq(value), eq(options))
        verify(wrapped).insertOne(eq(clientSession.wrapped), eq(value), refEq(defaultOptions))
        verify(wrapped).insertOne(eq(clientSession.wrapped), eq(value), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingInsertMany() {
        val mongoCollection = MongoCollection(wrapped)
        val value = listOf(Document("u", 1))
        val defaultOptions = InsertManyOptions()
        val options = InsertManyOptions().comment("comment")

        whenever(wrapped.insertMany(eq(value), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.insertMany(eq(value), eq(options))).doReturn(mock())
        whenever(wrapped.insertMany(eq(clientSession.wrapped), eq(value), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.insertMany(eq(clientSession.wrapped), eq(value), eq(options))).doReturn(mock())

        mongoCollection.insertMany(value)
        mongoCollection.insertMany(value, options)
        mongoCollection.insertMany(clientSession, value)
        mongoCollection.insertMany(clientSession, value, options)

        verify(wrapped).insertMany(eq(value), refEq(defaultOptions))
        verify(wrapped).insertMany(eq(value), eq(options))
        verify(wrapped).insertMany(eq(clientSession.wrapped), eq(value), refEq(defaultOptions))
        verify(wrapped).insertMany(eq(clientSession.wrapped), eq(value), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingBulkWrite() {
        val mongoCollection = MongoCollection(wrapped)
        val value = listOf(InsertOneModel(Document("u", 1)))
        val defaultOptions = BulkWriteOptions()
        val options = BulkWriteOptions().comment("comment")

        whenever(wrapped.bulkWrite(eq(value), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.bulkWrite(eq(value), eq(options))).doReturn(mock())
        whenever(wrapped.bulkWrite(eq(clientSession.wrapped), eq(value), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.bulkWrite(eq(clientSession.wrapped), eq(value), eq(options))).doReturn(mock())

        mongoCollection.bulkWrite(value)
        mongoCollection.bulkWrite(value, options)
        mongoCollection.bulkWrite(clientSession, value)
        mongoCollection.bulkWrite(clientSession, value, options)

        verify(wrapped).bulkWrite(eq(value), refEq(defaultOptions))
        verify(wrapped).bulkWrite(eq(value), eq(options))
        verify(wrapped).bulkWrite(eq(clientSession.wrapped), eq(value), refEq(defaultOptions))
        verify(wrapped).bulkWrite(eq(clientSession.wrapped), eq(value), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingUpdateOne() {
        val mongoCollection = MongoCollection(wrapped)
        val update = Document("u", 1)
        val updates = listOf(update)
        val defaultOptions = UpdateOptions()
        val options = UpdateOptions().comment("comment")

        whenever(wrapped.updateOne(eq(filter), eq(update), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.updateOne(eq(filter), eq(update), eq(options))).doReturn(mock())
        whenever(wrapped.updateOne(eq(filter), eq(updates), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.updateOne(eq(filter), eq(updates), eq(options))).doReturn(mock())
        whenever(wrapped.updateOne(eq(clientSession.wrapped), eq(filter), eq(update), refEq(defaultOptions)))
            .doReturn(mock())
        whenever(wrapped.updateOne(eq(clientSession.wrapped), eq(filter), eq(update), eq(options))).doReturn(mock())
        whenever(wrapped.updateOne(eq(clientSession.wrapped), eq(filter), eq(updates), refEq(defaultOptions)))
            .doReturn(mock())
        whenever(wrapped.updateOne(eq(clientSession.wrapped), eq(filter), eq(updates), eq(options))).doReturn(mock())

        mongoCollection.updateOne(filter, update)
        mongoCollection.updateOne(filter, update, options)
        mongoCollection.updateOne(filter, updates)
        mongoCollection.updateOne(filter, updates, options)
        mongoCollection.updateOne(clientSession, filter, update)
        mongoCollection.updateOne(clientSession, filter, update, options)
        mongoCollection.updateOne(clientSession, filter, updates)
        mongoCollection.updateOne(clientSession, filter, updates, options)

        verify(wrapped).updateOne(eq(filter), eq(update), refEq(defaultOptions))
        verify(wrapped).updateOne(eq(filter), eq(update), eq(options))
        verify(wrapped).updateOne(eq(filter), eq(updates), refEq(defaultOptions))
        verify(wrapped).updateOne(eq(filter), eq(updates), eq(options))
        verify(wrapped).updateOne(eq(clientSession.wrapped), eq(filter), eq(update), refEq(defaultOptions))
        verify(wrapped).updateOne(eq(clientSession.wrapped), eq(filter), eq(update), eq(options))
        verify(wrapped).updateOne(eq(clientSession.wrapped), eq(filter), eq(updates), refEq(defaultOptions))
        verify(wrapped).updateOne(eq(clientSession.wrapped), eq(filter), eq(updates), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingUpdateMany() {
        val mongoCollection = MongoCollection(wrapped)
        val update = Document("u", 1)
        val updates = listOf(update)
        val defaultOptions = UpdateOptions()
        val options = UpdateOptions().comment("comment")

        whenever(wrapped.updateMany(eq(filter), eq(update), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.updateMany(eq(filter), eq(update), eq(options))).doReturn(mock())
        whenever(wrapped.updateMany(eq(filter), eq(updates), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.updateMany(eq(filter), eq(updates), eq(options))).doReturn(mock())
        whenever(wrapped.updateMany(eq(clientSession.wrapped), eq(filter), eq(update), refEq(defaultOptions)))
            .doReturn(mock())
        whenever(wrapped.updateMany(eq(clientSession.wrapped), eq(filter), eq(update), eq(options))).doReturn(mock())
        whenever(wrapped.updateMany(eq(clientSession.wrapped), eq(filter), eq(updates), refEq(defaultOptions)))
            .doReturn(mock())
        whenever(wrapped.updateMany(eq(clientSession.wrapped), eq(filter), eq(updates), eq(options))).doReturn(mock())

        mongoCollection.updateMany(filter, update)
        mongoCollection.updateMany(filter, update, options)
        mongoCollection.updateMany(filter, updates)
        mongoCollection.updateMany(filter, updates, options)
        mongoCollection.updateMany(clientSession, filter, update)
        mongoCollection.updateMany(clientSession, filter, update, options)
        mongoCollection.updateMany(clientSession, filter, updates)
        mongoCollection.updateMany(clientSession, filter, updates, options)

        verify(wrapped).updateMany(eq(filter), eq(update), refEq(defaultOptions))
        verify(wrapped).updateMany(eq(filter), eq(update), eq(options))
        verify(wrapped).updateMany(eq(filter), eq(updates), refEq(defaultOptions))
        verify(wrapped).updateMany(eq(filter), eq(updates), eq(options))
        verify(wrapped).updateMany(eq(clientSession.wrapped), eq(filter), eq(update), refEq(defaultOptions))
        verify(wrapped).updateMany(eq(clientSession.wrapped), eq(filter), eq(update), eq(options))
        verify(wrapped).updateMany(eq(clientSession.wrapped), eq(filter), eq(updates), refEq(defaultOptions))
        verify(wrapped).updateMany(eq(clientSession.wrapped), eq(filter), eq(updates), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingReplaceOne() {
        val mongoCollection = MongoCollection(wrapped)
        val replacement = Document("u", 1)
        val defaultOptions = ReplaceOptions()
        val options = ReplaceOptions().comment("comment")

        whenever(wrapped.replaceOne(eq(filter), eq(replacement), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.replaceOne(eq(filter), eq(replacement), eq(options))).doReturn(mock())
        whenever(wrapped.replaceOne(eq(clientSession.wrapped), eq(filter), eq(replacement), refEq(defaultOptions)))
            .doReturn(mock())
        whenever(wrapped.replaceOne(eq(clientSession.wrapped), eq(filter), eq(replacement), eq(options)))
            .doReturn(mock())

        mongoCollection.replaceOne(filter, replacement)
        mongoCollection.replaceOne(filter, replacement, options)
        mongoCollection.replaceOne(clientSession, filter, replacement)
        mongoCollection.replaceOne(clientSession, filter, replacement, options)

        verify(wrapped).replaceOne(eq(filter), eq(replacement), refEq(defaultOptions))
        verify(wrapped).replaceOne(eq(filter), eq(replacement), eq(options))
        verify(wrapped).replaceOne(eq(clientSession.wrapped), eq(filter), eq(replacement), refEq(defaultOptions))
        verify(wrapped).replaceOne(eq(clientSession.wrapped), eq(filter), eq(replacement), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingDeleteOne() {
        val mongoCollection = MongoCollection(wrapped)

        val defaultOptions = DeleteOptions()
        val options = DeleteOptions().comment("comment")

        whenever(wrapped.deleteOne(eq(filter), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.deleteOne(eq(filter), eq(options))).doReturn(mock())
        whenever(wrapped.deleteOne(eq(clientSession.wrapped), eq(filter), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.deleteOne(eq(clientSession.wrapped), eq(filter), eq(options))).doReturn(mock())

        mongoCollection.deleteOne(filter)
        mongoCollection.deleteOne(filter, options)
        mongoCollection.deleteOne(clientSession, filter)
        mongoCollection.deleteOne(clientSession, filter, options)

        verify(wrapped).deleteOne(eq(filter), refEq(defaultOptions))
        verify(wrapped).deleteOne(eq(filter), eq(options))
        verify(wrapped).deleteOne(eq(clientSession.wrapped), eq(filter), refEq(defaultOptions))
        verify(wrapped).deleteOne(eq(clientSession.wrapped), eq(filter), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingDeleteMany() {
        val mongoCollection = MongoCollection(wrapped)

        val defaultOptions = DeleteOptions()
        val options = DeleteOptions().comment("comment")

        whenever(wrapped.deleteMany(eq(filter), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.deleteMany(eq(filter), eq(options))).doReturn(mock())
        whenever(wrapped.deleteMany(eq(clientSession.wrapped), eq(filter), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.deleteMany(eq(clientSession.wrapped), eq(filter), eq(options))).doReturn(mock())

        mongoCollection.deleteMany(filter)
        mongoCollection.deleteMany(filter, options)
        mongoCollection.deleteMany(clientSession, filter)
        mongoCollection.deleteMany(clientSession, filter, options)

        verify(wrapped).deleteMany(eq(filter), refEq(defaultOptions))
        verify(wrapped).deleteMany(eq(filter), eq(options))
        verify(wrapped).deleteMany(eq(clientSession.wrapped), eq(filter), refEq(defaultOptions))
        verify(wrapped).deleteMany(eq(clientSession.wrapped), eq(filter), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingFindOneAndDelete() {
        val mongoCollection = MongoCollection(wrapped)

        val defaultOptions = FindOneAndDeleteOptions()
        val options = FindOneAndDeleteOptions().comment("comment")

        whenever(wrapped.findOneAndDelete(eq(filter), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.findOneAndDelete(eq(filter), eq(options))).doReturn(mock())
        whenever(wrapped.findOneAndDelete(eq(clientSession.wrapped), eq(filter), refEq(defaultOptions)))
            .doReturn(mock())
        whenever(wrapped.findOneAndDelete(eq(clientSession.wrapped), eq(filter), eq(options))).doReturn(mock())

        mongoCollection.findOneAndDelete(filter)
        mongoCollection.findOneAndDelete(filter, options)
        mongoCollection.findOneAndDelete(clientSession, filter)
        mongoCollection.findOneAndDelete(clientSession, filter, options)

        verify(wrapped).findOneAndDelete(eq(filter), refEq(defaultOptions))
        verify(wrapped).findOneAndDelete(eq(filter), eq(options))
        verify(wrapped).findOneAndDelete(eq(clientSession.wrapped), eq(filter), refEq(defaultOptions))
        verify(wrapped).findOneAndDelete(eq(clientSession.wrapped), eq(filter), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingFindOneAndUpdate() {
        val mongoCollection = MongoCollection(wrapped)
        val update = Document("u", 1)
        val updateList = listOf(update)
        val defaultOptions = FindOneAndUpdateOptions()
        val options = FindOneAndUpdateOptions().comment("comment")

        whenever(wrapped.findOneAndUpdate(eq(filter), eq(update), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.findOneAndUpdate(eq(filter), eq(update), eq(options))).doReturn(mock())
        whenever(wrapped.findOneAndUpdate(eq(filter), eq(updateList), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.findOneAndUpdate(eq(filter), eq(updateList), eq(options))).doReturn(mock())
        whenever(wrapped.findOneAndUpdate(eq(clientSession.wrapped), eq(filter), eq(update), refEq(defaultOptions)))
            .doReturn(mock())
        whenever(wrapped.findOneAndUpdate(eq(clientSession.wrapped), eq(filter), eq(update), eq(options)))
            .doReturn(mock())
        whenever(wrapped.findOneAndUpdate(eq(clientSession.wrapped), eq(filter), eq(updateList), refEq(defaultOptions)))
            .doReturn(mock())
        whenever(wrapped.findOneAndUpdate(eq(clientSession.wrapped), eq(filter), eq(updateList), eq(options)))
            .doReturn(mock())

        mongoCollection.findOneAndUpdate(filter, update)
        mongoCollection.findOneAndUpdate(filter, update, options)
        mongoCollection.findOneAndUpdate(filter, updateList)
        mongoCollection.findOneAndUpdate(filter, updateList, options)
        mongoCollection.findOneAndUpdate(clientSession, filter, update)
        mongoCollection.findOneAndUpdate(clientSession, filter, update, options)
        mongoCollection.findOneAndUpdate(clientSession, filter, updateList)
        mongoCollection.findOneAndUpdate(clientSession, filter, updateList, options)

        verify(wrapped).findOneAndUpdate(eq(filter), eq(update), refEq(defaultOptions))
        verify(wrapped).findOneAndUpdate(eq(filter), eq(update), eq(options))
        verify(wrapped).findOneAndUpdate(eq(filter), eq(updateList), refEq(defaultOptions))
        verify(wrapped).findOneAndUpdate(eq(filter), eq(updateList), eq(options))
        verify(wrapped).findOneAndUpdate(eq(clientSession.wrapped), eq(filter), eq(update), refEq(defaultOptions))
        verify(wrapped).findOneAndUpdate(eq(clientSession.wrapped), eq(filter), eq(update), eq(options))
        verify(wrapped).findOneAndUpdate(eq(clientSession.wrapped), eq(filter), eq(updateList), refEq(defaultOptions))
        verify(wrapped).findOneAndUpdate(eq(clientSession.wrapped), eq(filter), eq(updateList), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingFindOneAndReplace() {
        val mongoCollection = MongoCollection(wrapped)
        val replacement = Document("u", 1)
        val defaultOptions = FindOneAndReplaceOptions()
        val options = FindOneAndReplaceOptions().comment("comment")

        whenever(wrapped.findOneAndReplace(eq(filter), eq(replacement), refEq(defaultOptions))).doReturn(mock())
        whenever(wrapped.findOneAndReplace(eq(filter), eq(replacement), eq(options))).doReturn(mock())
        whenever(
                wrapped.findOneAndReplace(
                    eq(clientSession.wrapped), eq(filter), eq(replacement), refEq(defaultOptions)))
            .doReturn(mock())
        whenever(wrapped.findOneAndReplace(eq(clientSession.wrapped), eq(filter), eq(replacement), eq(options)))
            .doReturn(mock())

        mongoCollection.findOneAndReplace(filter, replacement)
        mongoCollection.findOneAndReplace(filter, replacement, options)
        mongoCollection.findOneAndReplace(clientSession, filter, replacement)
        mongoCollection.findOneAndReplace(clientSession, filter, replacement, options)

        verify(wrapped).findOneAndReplace(eq(filter), eq(replacement), refEq(defaultOptions))
        verify(wrapped).findOneAndReplace(eq(filter), eq(replacement), eq(options))
        verify(wrapped).findOneAndReplace(eq(clientSession.wrapped), eq(filter), eq(replacement), refEq(defaultOptions))
        verify(wrapped).findOneAndReplace(eq(clientSession.wrapped), eq(filter), eq(replacement), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingDrop() {
        val mongoCollection = MongoCollection(wrapped)
        val defaultOptions = DropCollectionOptions()
        val options = DropCollectionOptions().encryptedFields(Document())

        mongoCollection.drop()
        mongoCollection.drop(options)
        mongoCollection.drop(clientSession)
        mongoCollection.drop(clientSession, options)

        verify(wrapped).drop(refEq(defaultOptions))
        verify(wrapped).drop(eq(options))
        verify(wrapped).drop(eq(clientSession.wrapped), refEq(defaultOptions))
        verify(wrapped).drop(eq(clientSession.wrapped), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingCreateIndex() {
        val mongoCollection = MongoCollection(wrapped)
        val key = Document()
        val defaultOptions = IndexOptions()
        val options = IndexOptions().name("name")

        whenever(wrapped.createIndex(eq(key), refEq(defaultOptions))).doReturn("1")
        whenever(wrapped.createIndex(eq(key), eq(options))).doReturn("2")
        whenever(wrapped.createIndex(eq(clientSession.wrapped), eq(key), refEq(defaultOptions))).doReturn("3")
        whenever(wrapped.createIndex(eq(clientSession.wrapped), eq(key), eq(options))).doReturn("4")

        assertEquals("1", mongoCollection.createIndex(key))
        assertEquals("2", mongoCollection.createIndex(key, options))
        assertEquals("3", mongoCollection.createIndex(clientSession, key))
        assertEquals("4", mongoCollection.createIndex(clientSession, key, options))

        verify(wrapped).createIndex(eq(key), refEq(defaultOptions))
        verify(wrapped).createIndex(eq(key), eq(options))
        verify(wrapped).createIndex(eq(clientSession.wrapped), eq(key), refEq(defaultOptions))
        verify(wrapped).createIndex(eq(clientSession.wrapped), eq(key), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingCreateIndexes() {
        val mongoCollection = MongoCollection(wrapped)
        val indexes = listOf(IndexModel(Document()))
        val defaultOptions = CreateIndexOptions()
        val options = CreateIndexOptions().commitQuorum(CreateIndexCommitQuorum.MAJORITY)

        whenever(wrapped.createIndexes(eq(indexes), refEq(defaultOptions))).doReturn(listOf("1"))
        whenever(wrapped.createIndexes(eq(indexes), eq(options))).doReturn(listOf("2"))
        whenever(wrapped.createIndexes(eq(clientSession.wrapped), eq(indexes), refEq(defaultOptions)))
            .doReturn(listOf("3"))
        whenever(wrapped.createIndexes(eq(clientSession.wrapped), eq(indexes), eq(options))).doReturn(listOf("4"))

        assertContentEquals(listOf("1"), mongoCollection.createIndexes(indexes))
        assertContentEquals(listOf("2"), mongoCollection.createIndexes(indexes, options))
        assertContentEquals(listOf("3"), mongoCollection.createIndexes(clientSession, indexes))
        assertContentEquals(listOf("4"), mongoCollection.createIndexes(clientSession, indexes, options))

        verify(wrapped).createIndexes(eq(indexes), refEq(defaultOptions))
        verify(wrapped).createIndexes(eq(indexes), eq(options))
        verify(wrapped).createIndexes(eq(clientSession.wrapped), eq(indexes), refEq(defaultOptions))
        verify(wrapped).createIndexes(eq(clientSession.wrapped), eq(indexes), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingListIndexes() {
        val mongoCollection = MongoCollection(wrapped)

        whenever(wrapped.listIndexes(Document::class.java)).doReturn(mock())
        whenever(wrapped.listIndexes(clientSession.wrapped, Document::class.java)).doReturn(mock())
        whenever(wrapped.listIndexes(BsonDocument::class.java)).doReturn(mock())
        whenever(wrapped.listIndexes(clientSession.wrapped, BsonDocument::class.java)).doReturn(mock())

        mongoCollection.listIndexes()
        mongoCollection.listIndexes(clientSession)

        mongoCollection.listIndexes(resultClass = Document::class.java)
        mongoCollection.listIndexes(clientSession, Document::class.java)

        mongoCollection.listIndexes<BsonDocument>()
        mongoCollection.listIndexes<BsonDocument>(clientSession)

        verify(wrapped, times(2)).listIndexes(Document::class.java)
        verify(wrapped, times(2)).listIndexes(clientSession.wrapped, Document::class.java)
        verify(wrapped, times(1)).listIndexes(BsonDocument::class.java)
        verify(wrapped, times(1)).listIndexes(clientSession.wrapped, BsonDocument::class.java)
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingDropIndex() {
        val mongoCollection = MongoCollection(wrapped)
        val indexName = "index"
        val keys = Document()
        val defaultOptions = DropIndexOptions()
        val options = DropIndexOptions().maxTime(1, TimeUnit.MILLISECONDS)

        mongoCollection.dropIndex(indexName)
        mongoCollection.dropIndex(indexName, options)
        mongoCollection.dropIndex(keys)
        mongoCollection.dropIndex(keys, options)
        mongoCollection.dropIndex(clientSession, indexName)
        mongoCollection.dropIndex(clientSession, indexName, options)
        mongoCollection.dropIndex(clientSession, keys)
        mongoCollection.dropIndex(clientSession, keys, options)

        verify(wrapped).dropIndex(eq(indexName), refEq(defaultOptions))
        verify(wrapped).dropIndex(eq(indexName), eq(options))
        verify(wrapped).dropIndex(eq(keys), refEq(defaultOptions))
        verify(wrapped).dropIndex(eq(keys), eq(options))
        verify(wrapped).dropIndex(eq(clientSession.wrapped), eq(indexName), refEq(defaultOptions))
        verify(wrapped).dropIndex(eq(clientSession.wrapped), eq(indexName), eq(options))
        verify(wrapped).dropIndex(eq(clientSession.wrapped), eq(keys), refEq(defaultOptions))
        verify(wrapped).dropIndex(eq(clientSession.wrapped), eq(keys), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingDropIndexes() {
        val mongoCollection = MongoCollection(wrapped)
        val defaultOptions = DropIndexOptions()
        val options = DropIndexOptions().maxTime(1, TimeUnit.MILLISECONDS)

        mongoCollection.dropIndexes()
        mongoCollection.dropIndexes(options)
        mongoCollection.dropIndexes(clientSession)
        mongoCollection.dropIndexes(clientSession, options)

        verify(wrapped).dropIndexes(refEq(defaultOptions))
        verify(wrapped).dropIndexes(eq(options))
        verify(wrapped).dropIndexes(eq(clientSession.wrapped), refEq(defaultOptions))
        verify(wrapped).dropIndexes(eq(clientSession.wrapped), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldCallTheUnderlyingRenameCollection() {
        val mongoCollection = MongoCollection(wrapped)
        val mongoNamespace = MongoNamespace("db", "coll")
        val defaultOptions = RenameCollectionOptions()
        val options = RenameCollectionOptions().dropTarget(true)

        mongoCollection.renameCollection(mongoNamespace)
        mongoCollection.renameCollection(mongoNamespace, options)
        mongoCollection.renameCollection(clientSession, mongoNamespace)
        mongoCollection.renameCollection(clientSession, mongoNamespace, options)

        verify(wrapped).renameCollection(eq(mongoNamespace), refEq(defaultOptions))
        verify(wrapped).renameCollection(eq(mongoNamespace), eq(options))
        verify(wrapped).renameCollection(eq(clientSession.wrapped), eq(mongoNamespace), refEq(defaultOptions))
        verify(wrapped).renameCollection(eq(clientSession.wrapped), eq(mongoNamespace), eq(options))
        verifyNoMoreInteractions(wrapped)
    }

    @Test
    fun shouldProvideExtensionFunctionsForTimeBasedOptions() {
        val oneThousand = 1000L

        assertEquals(1, CreateIndexOptions().maxTime(oneThousand).getMaxTime(TimeUnit.SECONDS))
        assertEquals(1, CountOptions().maxTime(oneThousand).getMaxTime(TimeUnit.SECONDS))
        assertEquals(1, DropIndexOptions().maxTime(oneThousand).getMaxTime(TimeUnit.SECONDS))
        assertEquals(1, EstimatedDocumentCountOptions().maxTime(oneThousand).getMaxTime(TimeUnit.SECONDS))
        assertEquals(1, FindOneAndDeleteOptions().maxTime(oneThousand).getMaxTime(TimeUnit.SECONDS))
        assertEquals(1, FindOneAndReplaceOptions().maxTime(oneThousand).getMaxTime(TimeUnit.SECONDS))
        assertEquals(1, FindOneAndUpdateOptions().maxTime(oneThousand).getMaxTime(TimeUnit.SECONDS))
        assertEquals(oneThousand, IndexOptions().expireAfter(oneThousand).getExpireAfter(TimeUnit.SECONDS))
    }
}
