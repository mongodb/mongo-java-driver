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
@file:Suppress("DEPRECATION")

package com.mongodb.kotlin.client.coroutine.syncadapter

import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.AggregateIterable
import com.mongodb.client.ChangeStreamIterable
import com.mongodb.client.ClientSession
import com.mongodb.client.DistinctIterable
import com.mongodb.client.FindIterable
import com.mongodb.client.ListIndexesIterable
import com.mongodb.client.MapReduceIterable
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
import com.mongodb.client.model.InsertOneOptions
import com.mongodb.client.model.RenameCollectionOptions
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.WriteModel
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.InsertManyResult
import com.mongodb.client.result.InsertOneResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.kotlin.client.coroutine.MongoCollection
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson

@Suppress("OVERRIDE_DEPRECATION")
data class SyncMongoCollection<T : Any>(val wrapped: MongoCollection<T>) : JMongoCollection<T> {
    override fun getNamespace(): MongoNamespace = wrapped.namespace

    override fun getDocumentClass(): Class<T> = wrapped.documentClass

    override fun getCodecRegistry(): CodecRegistry = wrapped.codecRegistry

    override fun getReadPreference(): ReadPreference = wrapped.readPreference

    override fun getWriteConcern(): WriteConcern = wrapped.writeConcern

    override fun getReadConcern(): ReadConcern = wrapped.readConcern

    override fun <R : Any> withDocumentClass(clazz: Class<R>): SyncMongoCollection<R> =
        SyncMongoCollection(wrapped.withDocumentClass(clazz))

    override fun withCodecRegistry(codecRegistry: CodecRegistry): SyncMongoCollection<T> =
        SyncMongoCollection(wrapped.withCodecRegistry(codecRegistry))

    override fun withReadPreference(readPreference: ReadPreference): SyncMongoCollection<T> =
        SyncMongoCollection(wrapped.withReadPreference(readPreference))

    override fun withWriteConcern(writeConcern: WriteConcern): SyncMongoCollection<T> =
        SyncMongoCollection(wrapped.withWriteConcern(writeConcern))

    override fun withReadConcern(readConcern: ReadConcern): SyncMongoCollection<T> =
        SyncMongoCollection(wrapped.withReadConcern(readConcern))

    override fun countDocuments(): Long = runBlocking { wrapped.countDocuments() }

    override fun countDocuments(filter: Bson): Long = runBlocking { wrapped.countDocuments(filter) }

    override fun countDocuments(filter: Bson, options: CountOptions): Long = runBlocking {
        wrapped.countDocuments(filter, options)
    }

    override fun countDocuments(clientSession: ClientSession): Long = runBlocking {
        wrapped.countDocuments(clientSession.unwrapped())
    }

    override fun countDocuments(clientSession: ClientSession, filter: Bson): Long = runBlocking {
        wrapped.countDocuments(clientSession.unwrapped(), filter)
    }

    override fun countDocuments(clientSession: ClientSession, filter: Bson, options: CountOptions): Long = runBlocking {
        wrapped.countDocuments(clientSession.unwrapped(), filter, options)
    }

    override fun estimatedDocumentCount(): Long = runBlocking { wrapped.estimatedDocumentCount() }

    override fun estimatedDocumentCount(options: EstimatedDocumentCountOptions): Long = runBlocking {
        wrapped.estimatedDocumentCount(options)
    }

    override fun <R : Any> distinct(fieldName: String, resultClass: Class<R>): DistinctIterable<R> =
        SyncDistinctIterable(wrapped.distinct(fieldName, resultClass = resultClass))

    override fun <R : Any> distinct(fieldName: String, filter: Bson, resultClass: Class<R>): DistinctIterable<R> =
        SyncDistinctIterable(wrapped.distinct(fieldName, filter, resultClass = resultClass))

    override fun <R : Any> distinct(
        clientSession: ClientSession,
        fieldName: String,
        resultClass: Class<R>
    ): DistinctIterable<R> =
        SyncDistinctIterable(wrapped.distinct(clientSession.unwrapped(), fieldName, resultClass = resultClass))

    override fun <R : Any> distinct(
        clientSession: ClientSession,
        fieldName: String,
        filter: Bson,
        resultClass: Class<R>
    ): DistinctIterable<R> =
        SyncDistinctIterable(wrapped.distinct(clientSession.unwrapped(), fieldName, filter, resultClass))

    override fun find(): FindIterable<T> = SyncFindIterable(wrapped.find())

    override fun <R : Any> find(resultClass: Class<R>): FindIterable<R> =
        SyncFindIterable(wrapped.find(resultClass = resultClass))

    override fun find(filter: Bson): FindIterable<T> = SyncFindIterable(wrapped.find(filter))

    override fun <R : Any> find(filter: Bson, resultClass: Class<R>): FindIterable<R> =
        SyncFindIterable(wrapped.find(filter, resultClass))

    override fun find(clientSession: ClientSession): FindIterable<T> =
        SyncFindIterable(wrapped.find(clientSession.unwrapped()))

    override fun <R : Any> find(clientSession: ClientSession, resultClass: Class<R>): FindIterable<R> =
        SyncFindIterable(wrapped.find(clientSession.unwrapped(), resultClass = resultClass))

    override fun find(clientSession: ClientSession, filter: Bson): FindIterable<T> =
        SyncFindIterable(wrapped.find(clientSession.unwrapped(), filter))

    override fun <R : Any> find(clientSession: ClientSession, filter: Bson, resultClass: Class<R>): FindIterable<R> =
        SyncFindIterable(wrapped.find(clientSession.unwrapped(), filter, resultClass))

    override fun aggregate(pipeline: MutableList<out Bson>): AggregateIterable<T> =
        SyncAggregateIterable(wrapped.aggregate(pipeline))

    override fun <R : Any> aggregate(pipeline: MutableList<out Bson>, resultClass: Class<R>): AggregateIterable<R> =
        SyncAggregateIterable(wrapped.aggregate(pipeline, resultClass))

    override fun aggregate(clientSession: ClientSession, pipeline: MutableList<out Bson>): AggregateIterable<T> =
        SyncAggregateIterable(wrapped.aggregate(clientSession.unwrapped(), pipeline))

    override fun <R : Any> aggregate(
        clientSession: ClientSession,
        pipeline: MutableList<out Bson>,
        resultClass: Class<R>
    ): AggregateIterable<R> = SyncAggregateIterable(wrapped.aggregate(clientSession.unwrapped(), pipeline, resultClass))

    override fun watch(): ChangeStreamIterable<T> = SyncChangeStreamIterable(wrapped.watch())

    override fun <R : Any> watch(resultClass: Class<R>): ChangeStreamIterable<R> =
        SyncChangeStreamIterable(wrapped.watch(resultClass = resultClass))

    override fun watch(pipeline: MutableList<out Bson>): ChangeStreamIterable<T> =
        SyncChangeStreamIterable(wrapped.watch(pipeline))

    override fun <R : Any> watch(pipeline: MutableList<out Bson>, resultClass: Class<R>): ChangeStreamIterable<R> =
        SyncChangeStreamIterable(wrapped.watch(pipeline, resultClass))

    override fun watch(clientSession: ClientSession): ChangeStreamIterable<T> =
        SyncChangeStreamIterable(wrapped.watch(clientSession.unwrapped()))

    override fun <R : Any> watch(clientSession: ClientSession, resultClass: Class<R>): ChangeStreamIterable<R> =
        SyncChangeStreamIterable(wrapped.watch(clientSession.unwrapped(), resultClass = resultClass))

    override fun watch(clientSession: ClientSession, pipeline: MutableList<out Bson>): ChangeStreamIterable<T> =
        SyncChangeStreamIterable(wrapped.watch(clientSession.unwrapped(), pipeline))

    override fun <R : Any> watch(
        clientSession: ClientSession,
        pipeline: MutableList<out Bson>,
        resultClass: Class<R>
    ): ChangeStreamIterable<R> =
        SyncChangeStreamIterable(wrapped.watch(clientSession.unwrapped(), pipeline, resultClass))

    override fun mapReduce(mapFunction: String, reduceFunction: String): MapReduceIterable<T> =
        SyncMapReduceIterable(wrapped.mapReduce(mapFunction, reduceFunction))

    override fun <R : Any> mapReduce(
        mapFunction: String,
        reduceFunction: String,
        resultClass: Class<R>
    ): MapReduceIterable<R> = SyncMapReduceIterable(wrapped.mapReduce(mapFunction, reduceFunction, resultClass))

    override fun mapReduce(
        clientSession: ClientSession,
        mapFunction: String,
        reduceFunction: String
    ): MapReduceIterable<T> =
        SyncMapReduceIterable(wrapped.mapReduce(clientSession.unwrapped(), mapFunction, reduceFunction))

    override fun <R : Any> mapReduce(
        clientSession: ClientSession,
        mapFunction: String,
        reduceFunction: String,
        resultClass: Class<R>
    ): MapReduceIterable<R> =
        SyncMapReduceIterable(wrapped.mapReduce(clientSession.unwrapped(), mapFunction, reduceFunction, resultClass))

    override fun deleteOne(filter: Bson): DeleteResult = runBlocking { wrapped.deleteOne(filter) }

    override fun deleteOne(filter: Bson, options: DeleteOptions): DeleteResult = runBlocking {
        wrapped.deleteOne(filter, options)
    }

    override fun deleteOne(clientSession: ClientSession, filter: Bson): DeleteResult = runBlocking {
        wrapped.deleteOne(clientSession.unwrapped(), filter)
    }

    override fun deleteOne(clientSession: ClientSession, filter: Bson, options: DeleteOptions): DeleteResult =
        runBlocking {
            wrapped.deleteOne(clientSession.unwrapped(), filter, options)
        }

    override fun deleteMany(filter: Bson): DeleteResult = runBlocking { wrapped.deleteMany(filter) }

    override fun deleteMany(filter: Bson, options: DeleteOptions): DeleteResult = runBlocking {
        wrapped.deleteMany(filter, options)
    }

    override fun deleteMany(clientSession: ClientSession, filter: Bson): DeleteResult = runBlocking {
        wrapped.deleteMany(clientSession.unwrapped(), filter)
    }

    override fun deleteMany(clientSession: ClientSession, filter: Bson, options: DeleteOptions): DeleteResult =
        runBlocking {
            wrapped.deleteMany(clientSession.unwrapped(), filter, options)
        }

    override fun updateOne(filter: Bson, update: Bson): UpdateResult = runBlocking { wrapped.updateOne(filter, update) }

    override fun updateOne(filter: Bson, update: Bson, updateOptions: UpdateOptions): UpdateResult = runBlocking {
        wrapped.updateOne(filter, update, updateOptions)
    }

    override fun updateOne(clientSession: ClientSession, filter: Bson, update: Bson): UpdateResult = runBlocking {
        wrapped.updateOne(clientSession.unwrapped(), filter, update)
    }

    override fun updateOne(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        updateOptions: UpdateOptions
    ): UpdateResult = runBlocking { wrapped.updateOne(clientSession.unwrapped(), filter, update, updateOptions) }

    override fun updateOne(filter: Bson, update: MutableList<out Bson>): UpdateResult = runBlocking {
        wrapped.updateOne(filter, update)
    }

    override fun updateOne(filter: Bson, update: MutableList<out Bson>, updateOptions: UpdateOptions): UpdateResult =
        runBlocking {
            wrapped.updateOne(filter, update, updateOptions)
        }

    override fun updateOne(clientSession: ClientSession, filter: Bson, update: MutableList<out Bson>): UpdateResult =
        runBlocking {
            wrapped.updateOne(clientSession.unwrapped(), filter, update)
        }

    override fun updateOne(
        clientSession: ClientSession,
        filter: Bson,
        update: MutableList<out Bson>,
        updateOptions: UpdateOptions
    ): UpdateResult = runBlocking { wrapped.updateOne(clientSession.unwrapped(), filter, update, updateOptions) }

    override fun updateMany(filter: Bson, update: Bson): UpdateResult = runBlocking {
        wrapped.updateMany(filter, update)
    }

    override fun updateMany(filter: Bson, update: Bson, updateOptions: UpdateOptions): UpdateResult = runBlocking {
        wrapped.updateMany(filter, update, updateOptions)
    }

    override fun updateMany(clientSession: ClientSession, filter: Bson, update: Bson): UpdateResult = runBlocking {
        wrapped.updateMany(clientSession.unwrapped(), filter, update)
    }

    override fun updateMany(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        updateOptions: UpdateOptions
    ): UpdateResult = runBlocking { wrapped.updateMany(clientSession.unwrapped(), filter, update, updateOptions) }

    override fun updateMany(filter: Bson, update: MutableList<out Bson>): UpdateResult = runBlocking {
        wrapped.updateMany(filter, update)
    }

    override fun updateMany(filter: Bson, update: MutableList<out Bson>, updateOptions: UpdateOptions): UpdateResult =
        runBlocking {
            wrapped.updateMany(filter, update, updateOptions)
        }

    override fun updateMany(clientSession: ClientSession, filter: Bson, update: MutableList<out Bson>): UpdateResult =
        runBlocking {
            wrapped.updateMany(clientSession.unwrapped(), filter, update)
        }

    override fun updateMany(
        clientSession: ClientSession,
        filter: Bson,
        update: MutableList<out Bson>,
        updateOptions: UpdateOptions
    ): UpdateResult = runBlocking { wrapped.updateMany(clientSession.unwrapped(), filter, update, updateOptions) }

    override fun findOneAndDelete(filter: Bson): T? = runBlocking { wrapped.findOneAndDelete(filter) }

    override fun findOneAndDelete(filter: Bson, options: FindOneAndDeleteOptions): T? = runBlocking {
        wrapped.findOneAndDelete(filter, options)
    }

    override fun findOneAndDelete(clientSession: ClientSession, filter: Bson): T? = runBlocking {
        wrapped.findOneAndDelete(clientSession.unwrapped(), filter)
    }

    override fun findOneAndDelete(clientSession: ClientSession, filter: Bson, options: FindOneAndDeleteOptions): T? =
        runBlocking {
            wrapped.findOneAndDelete(clientSession.unwrapped(), filter, options)
        }

    override fun findOneAndUpdate(filter: Bson, update: Bson): T? = runBlocking {
        wrapped.findOneAndUpdate(filter, update)
    }

    override fun findOneAndUpdate(filter: Bson, update: Bson, options: FindOneAndUpdateOptions): T? = runBlocking {
        wrapped.findOneAndUpdate(filter, update, options)
    }

    override fun findOneAndUpdate(clientSession: ClientSession, filter: Bson, update: Bson): T? = runBlocking {
        wrapped.findOneAndUpdate(clientSession.unwrapped(), filter, update)
    }

    override fun findOneAndUpdate(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        options: FindOneAndUpdateOptions
    ): T? = runBlocking { wrapped.findOneAndUpdate(clientSession.unwrapped(), filter, update, options) }

    override fun findOneAndUpdate(filter: Bson, update: MutableList<out Bson>): T? = runBlocking {
        wrapped.findOneAndUpdate(filter, update)
    }

    override fun findOneAndUpdate(filter: Bson, update: MutableList<out Bson>, options: FindOneAndUpdateOptions): T? =
        runBlocking {
            wrapped.findOneAndUpdate(filter, update, options)
        }

    override fun findOneAndUpdate(clientSession: ClientSession, filter: Bson, update: MutableList<out Bson>): T? =
        runBlocking {
            wrapped.findOneAndUpdate(clientSession.unwrapped(), filter, update)
        }

    override fun findOneAndUpdate(
        clientSession: ClientSession,
        filter: Bson,
        update: MutableList<out Bson>,
        options: FindOneAndUpdateOptions
    ): T? = runBlocking { wrapped.findOneAndUpdate(clientSession.unwrapped(), filter, update, options) }

    override fun drop() = runBlocking { wrapped.drop() }

    override fun drop(clientSession: ClientSession) = runBlocking { wrapped.drop(clientSession.unwrapped()) }

    override fun drop(dropCollectionOptions: DropCollectionOptions) = runBlocking {
        wrapped.drop(dropCollectionOptions)
    }

    override fun drop(clientSession: ClientSession, dropCollectionOptions: DropCollectionOptions) = runBlocking {
        wrapped.drop(clientSession.unwrapped(), dropCollectionOptions)
    }

    override fun createIndex(keys: Bson): String = runBlocking { wrapped.createIndex(keys) }

    override fun createIndex(keys: Bson, indexOptions: IndexOptions): String = runBlocking {
        wrapped.createIndex(keys, indexOptions)
    }

    override fun createIndex(clientSession: ClientSession, keys: Bson): String = runBlocking {
        wrapped.createIndex(clientSession.unwrapped(), keys)
    }

    override fun createIndex(clientSession: ClientSession, keys: Bson, indexOptions: IndexOptions): String =
        runBlocking {
            wrapped.createIndex(clientSession.unwrapped(), keys, indexOptions)
        }

    override fun createIndexes(indexes: MutableList<IndexModel>): MutableList<String> = runBlocking {
        wrapped.createIndexes(indexes).toCollection(mutableListOf())
    }

    override fun createIndexes(
        indexes: MutableList<IndexModel>,
        createIndexOptions: CreateIndexOptions
    ): MutableList<String> = runBlocking {
        wrapped.createIndexes(indexes, createIndexOptions).toCollection(mutableListOf())
    }

    override fun createIndexes(clientSession: ClientSession, indexes: MutableList<IndexModel>): MutableList<String> =
        runBlocking {
            wrapped.createIndexes(clientSession.unwrapped(), indexes).toCollection(mutableListOf())
        }

    override fun createIndexes(
        clientSession: ClientSession,
        indexes: MutableList<IndexModel>,
        createIndexOptions: CreateIndexOptions
    ): MutableList<String> = runBlocking {
        wrapped.createIndexes(clientSession.unwrapped(), indexes, createIndexOptions).toCollection(mutableListOf())
    }

    override fun listIndexes(): ListIndexesIterable<Document> = SyncListIndexesIterable(wrapped.listIndexes())

    override fun <R : Any> listIndexes(resultClass: Class<R>): ListIndexesIterable<R> =
        SyncListIndexesIterable(wrapped.listIndexes(resultClass = resultClass))

    override fun listIndexes(clientSession: ClientSession): ListIndexesIterable<Document> =
        SyncListIndexesIterable(wrapped.listIndexes(clientSession.unwrapped()))

    override fun <R : Any> listIndexes(clientSession: ClientSession, resultClass: Class<R>): ListIndexesIterable<R> =
        SyncListIndexesIterable(wrapped.listIndexes(clientSession.unwrapped(), resultClass))

    override fun dropIndex(indexName: String) = runBlocking { wrapped.dropIndex(indexName) }

    override fun dropIndex(indexName: String, dropIndexOptions: DropIndexOptions) = runBlocking {
        wrapped.dropIndex(indexName, dropIndexOptions)
    }

    override fun dropIndex(keys: Bson) = runBlocking { wrapped.dropIndex(keys) }

    override fun dropIndex(keys: Bson, dropIndexOptions: DropIndexOptions) = runBlocking {
        wrapped.dropIndex(keys, dropIndexOptions)
    }

    override fun dropIndex(clientSession: ClientSession, indexName: String) = runBlocking {
        wrapped.dropIndex(clientSession.unwrapped(), indexName)
    }

    override fun dropIndex(clientSession: ClientSession, keys: Bson) = runBlocking {
        wrapped.dropIndex(clientSession.unwrapped(), keys)
    }
    override fun dropIndex(clientSession: ClientSession, indexName: String, dropIndexOptions: DropIndexOptions) =
        runBlocking {
            wrapped.dropIndex(clientSession.unwrapped(), indexName, dropIndexOptions)
        }

    override fun dropIndex(clientSession: ClientSession, keys: Bson, dropIndexOptions: DropIndexOptions) = runBlocking {
        wrapped.dropIndex(clientSession.unwrapped(), keys, dropIndexOptions)
    }

    override fun dropIndexes() = runBlocking { wrapped.dropIndexes() }

    override fun dropIndexes(clientSession: ClientSession) = runBlocking {
        wrapped.dropIndexes(clientSession.unwrapped())
    }

    override fun dropIndexes(dropIndexOptions: DropIndexOptions) = runBlocking { wrapped.dropIndexes(dropIndexOptions) }

    override fun dropIndexes(clientSession: ClientSession, dropIndexOptions: DropIndexOptions) = runBlocking {
        wrapped.dropIndexes(clientSession.unwrapped(), dropIndexOptions)
    }

    override fun renameCollection(newCollectionNamespace: MongoNamespace) = runBlocking {
        wrapped.renameCollection(newCollectionNamespace)
    }

    override fun renameCollection(
        newCollectionNamespace: MongoNamespace,
        renameCollectionOptions: RenameCollectionOptions
    ) = runBlocking { wrapped.renameCollection(newCollectionNamespace, renameCollectionOptions) }

    override fun renameCollection(clientSession: ClientSession, newCollectionNamespace: MongoNamespace) = runBlocking {
        wrapped.renameCollection(clientSession.unwrapped(), newCollectionNamespace)
    }

    override fun renameCollection(
        clientSession: ClientSession,
        newCollectionNamespace: MongoNamespace,
        renameCollectionOptions: RenameCollectionOptions
    ) = runBlocking {
        wrapped.renameCollection(clientSession.unwrapped(), newCollectionNamespace, renameCollectionOptions)
    }

    override fun findOneAndReplace(
        clientSession: ClientSession,
        filter: Bson,
        replacement: T,
        options: FindOneAndReplaceOptions
    ): T? = runBlocking { wrapped.findOneAndReplace(clientSession.unwrapped(), filter, replacement, options) }

    override fun findOneAndReplace(clientSession: ClientSession, filter: Bson, replacement: T): T? = runBlocking {
        wrapped.findOneAndReplace(clientSession.unwrapped(), filter, replacement)
    }

    override fun findOneAndReplace(filter: Bson, replacement: T, options: FindOneAndReplaceOptions): T? = runBlocking {
        wrapped.findOneAndReplace(filter, replacement, options)
    }

    override fun findOneAndReplace(filter: Bson, replacement: T): T? = runBlocking {
        wrapped.findOneAndReplace(filter, replacement)
    }

    override fun replaceOne(
        clientSession: ClientSession,
        filter: Bson,
        replacement: T,
        replaceOptions: ReplaceOptions
    ): UpdateResult = runBlocking { wrapped.replaceOne(clientSession.unwrapped(), filter, replacement, replaceOptions) }

    override fun replaceOne(clientSession: ClientSession, filter: Bson, replacement: T): UpdateResult = runBlocking {
        wrapped.replaceOne(clientSession.unwrapped(), filter, replacement)
    }

    override fun replaceOne(filter: Bson, replacement: T, replaceOptions: ReplaceOptions): UpdateResult = runBlocking {
        wrapped.replaceOne(filter, replacement, replaceOptions)
    }

    override fun replaceOne(filter: Bson, replacement: T): UpdateResult = runBlocking {
        wrapped.replaceOne(filter, replacement)
    }

    override fun insertMany(
        clientSession: ClientSession,
        documents: MutableList<out T>,
        options: InsertManyOptions
    ): InsertManyResult = runBlocking { wrapped.insertMany(clientSession.unwrapped(), documents, options) }

    override fun insertMany(clientSession: ClientSession, documents: MutableList<out T>): InsertManyResult =
        runBlocking {
            wrapped.insertMany(clientSession.unwrapped(), documents)
        }

    override fun insertMany(documents: MutableList<out T>, options: InsertManyOptions): InsertManyResult = runBlocking {
        wrapped.insertMany(documents, options)
    }

    override fun insertMany(documents: MutableList<out T>): InsertManyResult = runBlocking {
        wrapped.insertMany(documents)
    }

    override fun insertOne(clientSession: ClientSession, document: T, options: InsertOneOptions): InsertOneResult =
        runBlocking {
            wrapped.insertOne(clientSession.unwrapped(), document, options)
        }

    override fun insertOne(clientSession: ClientSession, document: T): InsertOneResult = runBlocking {
        wrapped.insertOne(clientSession.unwrapped(), document)
    }

    override fun insertOne(document: T, options: InsertOneOptions): InsertOneResult = runBlocking {
        wrapped.insertOne(document, options)
    }

    override fun insertOne(document: T): InsertOneResult = runBlocking { wrapped.insertOne(document) }

    override fun bulkWrite(
        clientSession: ClientSession,
        requests: MutableList<out WriteModel<out T>>,
        options: BulkWriteOptions
    ): BulkWriteResult = runBlocking { wrapped.bulkWrite(clientSession.unwrapped(), requests, options) }

    override fun bulkWrite(
        clientSession: ClientSession,
        requests: MutableList<out WriteModel<out T>>
    ): BulkWriteResult = runBlocking { wrapped.bulkWrite(clientSession.unwrapped(), requests) }

    override fun bulkWrite(requests: MutableList<out WriteModel<out T>>, options: BulkWriteOptions): BulkWriteResult =
        runBlocking {
            wrapped.bulkWrite(requests, options)
        }

    override fun bulkWrite(requests: MutableList<out WriteModel<out T>>): BulkWriteResult = runBlocking {
        wrapped.bulkWrite(requests)
    }

    private fun ClientSession.unwrapped() = (this as SyncClientSession).wrapped
}
