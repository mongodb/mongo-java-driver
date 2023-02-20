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

package com.mongodb.kotlin.client.syncadapter

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
import com.mongodb.kotlin.client.MongoCollection
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

    override fun countDocuments(): Long = wrapped.countDocuments()

    override fun countDocuments(filter: Bson): Long = wrapped.countDocuments(filter)

    override fun countDocuments(filter: Bson, options: CountOptions): Long = wrapped.countDocuments(filter, options)

    override fun countDocuments(clientSession: ClientSession): Long = wrapped.countDocuments(clientSession)

    override fun countDocuments(clientSession: ClientSession, filter: Bson): Long =
        wrapped.countDocuments(clientSession, filter)

    override fun countDocuments(clientSession: ClientSession, filter: Bson, options: CountOptions): Long =
        wrapped.countDocuments(clientSession, filter, options)

    override fun estimatedDocumentCount(): Long = wrapped.estimatedDocumentCount()

    override fun estimatedDocumentCount(options: EstimatedDocumentCountOptions): Long =
        wrapped.estimatedDocumentCount(options)

    override fun <R : Any> distinct(fieldName: String, resultClass: Class<R>): DistinctIterable<R> =
        SyncDistinctIterable(wrapped.distinct(fieldName, resultClass = resultClass))

    override fun <R : Any> distinct(fieldName: String, filter: Bson, resultClass: Class<R>): DistinctIterable<R> =
        SyncDistinctIterable(wrapped.distinct(fieldName, filter, resultClass = resultClass))

    override fun <R : Any> distinct(
        clientSession: ClientSession,
        fieldName: String,
        resultClass: Class<R>
    ): DistinctIterable<R> = SyncDistinctIterable(wrapped.distinct(clientSession, fieldName, resultClass = resultClass))

    override fun <R : Any> distinct(
        clientSession: ClientSession,
        fieldName: String,
        filter: Bson,
        resultClass: Class<R>
    ): DistinctIterable<R> = SyncDistinctIterable(wrapped.distinct(clientSession, fieldName, filter, resultClass))

    override fun find(): FindIterable<T> = SyncFindIterable(wrapped.find())

    override fun <R : Any> find(resultClass: Class<R>): FindIterable<R> =
        SyncFindIterable(wrapped.find(resultClass = resultClass))

    override fun find(filter: Bson): FindIterable<T> = SyncFindIterable(wrapped.find(filter))

    override fun <R : Any> find(filter: Bson, resultClass: Class<R>): FindIterable<R> =
        SyncFindIterable(wrapped.find(filter, resultClass))

    override fun find(clientSession: ClientSession): FindIterable<T> = SyncFindIterable(wrapped.find(clientSession))

    override fun <R : Any> find(clientSession: ClientSession, resultClass: Class<R>): FindIterable<R> =
        SyncFindIterable(wrapped.find(clientSession, resultClass = resultClass))

    override fun find(clientSession: ClientSession, filter: Bson): FindIterable<T> =
        SyncFindIterable(wrapped.find(clientSession, filter))

    override fun <R : Any> find(clientSession: ClientSession, filter: Bson, resultClass: Class<R>): FindIterable<R> =
        SyncFindIterable(wrapped.find(clientSession, filter, resultClass))

    override fun aggregate(pipeline: MutableList<out Bson>): AggregateIterable<T> =
        SyncAggregateIterable(wrapped.aggregate(pipeline))

    override fun <R : Any> aggregate(pipeline: MutableList<out Bson>, resultClass: Class<R>): AggregateIterable<R> =
        SyncAggregateIterable(wrapped.aggregate(pipeline, resultClass))

    override fun aggregate(clientSession: ClientSession, pipeline: MutableList<out Bson>): AggregateIterable<T> =
        SyncAggregateIterable(wrapped.aggregate(clientSession, pipeline))

    override fun <R : Any> aggregate(
        clientSession: ClientSession,
        pipeline: MutableList<out Bson>,
        resultClass: Class<R>
    ): AggregateIterable<R> = SyncAggregateIterable(wrapped.aggregate(clientSession, pipeline, resultClass))

    override fun watch(): ChangeStreamIterable<T> = SyncChangeStreamIterable(wrapped.watch())

    override fun <R : Any> watch(resultClass: Class<R>): ChangeStreamIterable<R> =
        SyncChangeStreamIterable(wrapped.watch(resultClass = resultClass))

    override fun watch(pipeline: MutableList<out Bson>): ChangeStreamIterable<T> =
        SyncChangeStreamIterable(wrapped.watch(pipeline))

    override fun <R : Any> watch(pipeline: MutableList<out Bson>, resultClass: Class<R>): ChangeStreamIterable<R> =
        SyncChangeStreamIterable(wrapped.watch(pipeline, resultClass))

    override fun watch(clientSession: ClientSession): ChangeStreamIterable<T> =
        SyncChangeStreamIterable(wrapped.watch(clientSession))

    override fun <R : Any> watch(clientSession: ClientSession, resultClass: Class<R>): ChangeStreamIterable<R> =
        SyncChangeStreamIterable(wrapped.watch(clientSession, resultClass = resultClass))

    override fun watch(clientSession: ClientSession, pipeline: MutableList<out Bson>): ChangeStreamIterable<T> =
        SyncChangeStreamIterable(wrapped.watch(clientSession, pipeline))

    override fun <R : Any> watch(
        clientSession: ClientSession,
        pipeline: MutableList<out Bson>,
        resultClass: Class<R>
    ): ChangeStreamIterable<R> = SyncChangeStreamIterable(wrapped.watch(clientSession, pipeline, resultClass))

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
    ): MapReduceIterable<T> = SyncMapReduceIterable(wrapped.mapReduce(clientSession, mapFunction, reduceFunction))

    override fun <R : Any> mapReduce(
        clientSession: ClientSession,
        mapFunction: String,
        reduceFunction: String,
        resultClass: Class<R>
    ): MapReduceIterable<R> =
        SyncMapReduceIterable(wrapped.mapReduce(clientSession, mapFunction, reduceFunction, resultClass))

    override fun deleteOne(filter: Bson): DeleteResult = wrapped.deleteOne(filter)

    override fun deleteOne(filter: Bson, options: DeleteOptions): DeleteResult = wrapped.deleteOne(filter, options)

    override fun deleteOne(clientSession: ClientSession, filter: Bson): DeleteResult =
        wrapped.deleteOne(clientSession, filter)

    override fun deleteOne(clientSession: ClientSession, filter: Bson, options: DeleteOptions): DeleteResult =
        wrapped.deleteOne(clientSession, filter, options)

    override fun deleteMany(filter: Bson): DeleteResult = wrapped.deleteMany(filter)

    override fun deleteMany(filter: Bson, options: DeleteOptions): DeleteResult = wrapped.deleteMany(filter, options)

    override fun deleteMany(clientSession: ClientSession, filter: Bson): DeleteResult =
        wrapped.deleteMany(clientSession, filter)

    override fun deleteMany(clientSession: ClientSession, filter: Bson, options: DeleteOptions): DeleteResult =
        wrapped.deleteMany(clientSession, filter, options)

    override fun updateOne(filter: Bson, update: Bson): UpdateResult = wrapped.updateOne(filter, update)

    override fun updateOne(filter: Bson, update: Bson, updateOptions: UpdateOptions): UpdateResult =
        wrapped.updateOne(filter, update, updateOptions)

    override fun updateOne(clientSession: ClientSession, filter: Bson, update: Bson): UpdateResult =
        wrapped.updateOne(clientSession, filter, update)

    override fun updateOne(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        updateOptions: UpdateOptions
    ): UpdateResult = wrapped.updateOne(clientSession, filter, update, updateOptions)

    override fun updateOne(filter: Bson, update: MutableList<out Bson>): UpdateResult =
        wrapped.updateOne(filter, update)

    override fun updateOne(filter: Bson, update: MutableList<out Bson>, updateOptions: UpdateOptions): UpdateResult =
        wrapped.updateOne(filter, update, updateOptions)

    override fun updateOne(clientSession: ClientSession, filter: Bson, update: MutableList<out Bson>): UpdateResult =
        wrapped.updateOne(clientSession, filter, update)

    override fun updateOne(
        clientSession: ClientSession,
        filter: Bson,
        update: MutableList<out Bson>,
        updateOptions: UpdateOptions
    ): UpdateResult = wrapped.updateOne(clientSession, filter, update, updateOptions)

    override fun updateMany(filter: Bson, update: Bson): UpdateResult = wrapped.updateMany(filter, update)

    override fun updateMany(filter: Bson, update: Bson, updateOptions: UpdateOptions): UpdateResult =
        wrapped.updateMany(filter, update, updateOptions)

    override fun updateMany(clientSession: ClientSession, filter: Bson, update: Bson): UpdateResult =
        wrapped.updateMany(clientSession, filter, update)

    override fun updateMany(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        updateOptions: UpdateOptions
    ): UpdateResult = wrapped.updateMany(clientSession, filter, update, updateOptions)

    override fun updateMany(filter: Bson, update: MutableList<out Bson>): UpdateResult =
        wrapped.updateMany(filter, update)

    override fun updateMany(filter: Bson, update: MutableList<out Bson>, updateOptions: UpdateOptions): UpdateResult =
        wrapped.updateMany(filter, update, updateOptions)

    override fun updateMany(clientSession: ClientSession, filter: Bson, update: MutableList<out Bson>): UpdateResult =
        wrapped.updateMany(clientSession, filter, update)

    override fun updateMany(
        clientSession: ClientSession,
        filter: Bson,
        update: MutableList<out Bson>,
        updateOptions: UpdateOptions
    ): UpdateResult = wrapped.updateMany(clientSession, filter, update, updateOptions)

    override fun findOneAndDelete(filter: Bson): T? = wrapped.findOneAndDelete(filter)

    override fun findOneAndDelete(filter: Bson, options: FindOneAndDeleteOptions): T? =
        wrapped.findOneAndDelete(filter, options)

    override fun findOneAndDelete(clientSession: ClientSession, filter: Bson): T? =
        wrapped.findOneAndDelete(clientSession, filter)

    override fun findOneAndDelete(clientSession: ClientSession, filter: Bson, options: FindOneAndDeleteOptions): T? =
        wrapped.findOneAndDelete(clientSession, filter, options)

    override fun findOneAndUpdate(filter: Bson, update: Bson): T? = wrapped.findOneAndUpdate(filter, update)

    override fun findOneAndUpdate(filter: Bson, update: Bson, options: FindOneAndUpdateOptions): T? =
        wrapped.findOneAndUpdate(filter, update, options)

    override fun findOneAndUpdate(clientSession: ClientSession, filter: Bson, update: Bson): T? =
        wrapped.findOneAndUpdate(clientSession, filter, update)

    override fun findOneAndUpdate(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        options: FindOneAndUpdateOptions
    ): T? = wrapped.findOneAndUpdate(clientSession, filter, update, options)

    override fun findOneAndUpdate(filter: Bson, update: MutableList<out Bson>): T? =
        wrapped.findOneAndUpdate(filter, update)

    override fun findOneAndUpdate(filter: Bson, update: MutableList<out Bson>, options: FindOneAndUpdateOptions): T? =
        wrapped.findOneAndUpdate(filter, update, options)

    override fun findOneAndUpdate(clientSession: ClientSession, filter: Bson, update: MutableList<out Bson>): T? =
        wrapped.findOneAndUpdate(clientSession, filter, update)

    override fun findOneAndUpdate(
        clientSession: ClientSession,
        filter: Bson,
        update: MutableList<out Bson>,
        options: FindOneAndUpdateOptions
    ): T? = wrapped.findOneAndUpdate(clientSession, filter, update, options)

    override fun drop() = wrapped.drop()

    override fun drop(clientSession: ClientSession) = wrapped.drop(clientSession)

    override fun drop(dropCollectionOptions: DropCollectionOptions) = wrapped.drop(dropCollectionOptions)

    override fun drop(clientSession: ClientSession, dropCollectionOptions: DropCollectionOptions) =
        wrapped.drop(clientSession, dropCollectionOptions)

    override fun createIndex(keys: Bson): String = wrapped.createIndex(keys)

    override fun createIndex(keys: Bson, indexOptions: IndexOptions): String = wrapped.createIndex(keys, indexOptions)

    override fun createIndex(clientSession: ClientSession, keys: Bson): String =
        wrapped.createIndex(clientSession, keys)

    override fun createIndex(clientSession: ClientSession, keys: Bson, indexOptions: IndexOptions): String =
        wrapped.createIndex(clientSession, keys, indexOptions)

    override fun createIndexes(indexes: MutableList<IndexModel>): MutableList<String> =
        wrapped.createIndexes(indexes).toMutableList()

    override fun createIndexes(
        indexes: MutableList<IndexModel>,
        createIndexOptions: CreateIndexOptions
    ): MutableList<String> = wrapped.createIndexes(indexes, createIndexOptions).toMutableList()

    override fun createIndexes(clientSession: ClientSession, indexes: MutableList<IndexModel>): MutableList<String> =
        wrapped.createIndexes(clientSession, indexes).toMutableList()

    override fun createIndexes(
        clientSession: ClientSession,
        indexes: MutableList<IndexModel>,
        createIndexOptions: CreateIndexOptions
    ): MutableList<String> = wrapped.createIndexes(clientSession, indexes, createIndexOptions).toMutableList()

    override fun listIndexes(): ListIndexesIterable<Document> = SyncListIndexesIterable(wrapped.listIndexes())

    override fun <R : Any> listIndexes(resultClass: Class<R>): ListIndexesIterable<R> =
        SyncListIndexesIterable(wrapped.listIndexes(resultClass = resultClass))

    override fun listIndexes(clientSession: ClientSession): ListIndexesIterable<Document> =
        SyncListIndexesIterable(wrapped.listIndexes(clientSession))

    override fun <R : Any> listIndexes(clientSession: ClientSession, resultClass: Class<R>): ListIndexesIterable<R> =
        SyncListIndexesIterable(wrapped.listIndexes(clientSession, resultClass))

    override fun dropIndex(indexName: String) = wrapped.dropIndex(indexName)

    override fun dropIndex(indexName: String, dropIndexOptions: DropIndexOptions) =
        wrapped.dropIndex(indexName, dropIndexOptions)

    override fun dropIndex(keys: Bson) = wrapped.dropIndex(keys)

    override fun dropIndex(keys: Bson, dropIndexOptions: DropIndexOptions) = wrapped.dropIndex(keys, dropIndexOptions)

    override fun dropIndex(clientSession: ClientSession, indexName: String) =
        wrapped.dropIndex(clientSession, indexName)

    override fun dropIndex(clientSession: ClientSession, keys: Bson) = wrapped.dropIndex(clientSession, keys)
    override fun dropIndex(clientSession: ClientSession, indexName: String, dropIndexOptions: DropIndexOptions) =
        wrapped.dropIndex(clientSession, indexName, dropIndexOptions)

    override fun dropIndex(clientSession: ClientSession, keys: Bson, dropIndexOptions: DropIndexOptions) =
        wrapped.dropIndex(clientSession, keys, dropIndexOptions)

    override fun dropIndexes() = wrapped.dropIndexes()

    override fun dropIndexes(clientSession: ClientSession) = wrapped.dropIndexes(clientSession)

    override fun dropIndexes(dropIndexOptions: DropIndexOptions) = wrapped.dropIndexes(dropIndexOptions)

    override fun dropIndexes(clientSession: ClientSession, dropIndexOptions: DropIndexOptions) =
        wrapped.dropIndexes(clientSession, dropIndexOptions)

    override fun renameCollection(newCollectionNamespace: MongoNamespace) =
        wrapped.renameCollection(newCollectionNamespace)

    override fun renameCollection(
        newCollectionNamespace: MongoNamespace,
        renameCollectionOptions: RenameCollectionOptions
    ) = wrapped.renameCollection(newCollectionNamespace, renameCollectionOptions)

    override fun renameCollection(clientSession: ClientSession, newCollectionNamespace: MongoNamespace) =
        wrapped.renameCollection(clientSession, newCollectionNamespace)

    override fun renameCollection(
        clientSession: ClientSession,
        newCollectionNamespace: MongoNamespace,
        renameCollectionOptions: RenameCollectionOptions
    ) = wrapped.renameCollection(clientSession, newCollectionNamespace, renameCollectionOptions)
    override fun findOneAndReplace(
        clientSession: ClientSession,
        filter: Bson,
        replacement: T,
        options: FindOneAndReplaceOptions
    ): T? = wrapped.findOneAndReplace(clientSession, filter, replacement, options)

    override fun findOneAndReplace(clientSession: ClientSession, filter: Bson, replacement: T): T? =
        wrapped.findOneAndReplace(clientSession, filter, replacement)

    override fun findOneAndReplace(filter: Bson, replacement: T, options: FindOneAndReplaceOptions): T? =
        wrapped.findOneAndReplace(filter, replacement, options)

    override fun findOneAndReplace(filter: Bson, replacement: T): T? = wrapped.findOneAndReplace(filter, replacement)

    override fun replaceOne(
        clientSession: ClientSession,
        filter: Bson,
        replacement: T,
        replaceOptions: ReplaceOptions
    ): UpdateResult = wrapped.replaceOne(clientSession, filter, replacement, replaceOptions)

    override fun replaceOne(clientSession: ClientSession, filter: Bson, replacement: T): UpdateResult =
        wrapped.replaceOne(clientSession, filter, replacement)

    override fun replaceOne(filter: Bson, replacement: T, replaceOptions: ReplaceOptions): UpdateResult =
        wrapped.replaceOne(filter, replacement, replaceOptions)

    override fun replaceOne(filter: Bson, replacement: T): UpdateResult = wrapped.replaceOne(filter, replacement)

    override fun insertMany(
        clientSession: ClientSession,
        documents: MutableList<out T>,
        options: InsertManyOptions
    ): InsertManyResult = wrapped.insertMany(clientSession, documents, options)

    override fun insertMany(clientSession: ClientSession, documents: MutableList<out T>): InsertManyResult =
        wrapped.insertMany(clientSession, documents)

    override fun insertMany(documents: MutableList<out T>, options: InsertManyOptions): InsertManyResult =
        wrapped.insertMany(documents, options)

    override fun insertMany(documents: MutableList<out T>): InsertManyResult = wrapped.insertMany(documents)

    override fun insertOne(clientSession: ClientSession, document: T, options: InsertOneOptions): InsertOneResult =
        wrapped.insertOne(clientSession, document, options)

    override fun insertOne(clientSession: ClientSession, document: T): InsertOneResult =
        wrapped.insertOne(clientSession, document)

    override fun insertOne(document: T, options: InsertOneOptions): InsertOneResult =
        wrapped.insertOne(document, options)

    override fun insertOne(document: T): InsertOneResult = wrapped.insertOne(document)

    override fun bulkWrite(
        clientSession: ClientSession,
        requests: MutableList<out WriteModel<out T>>,
        options: BulkWriteOptions
    ): BulkWriteResult = wrapped.bulkWrite(clientSession, requests, options)

    override fun bulkWrite(
        clientSession: ClientSession,
        requests: MutableList<out WriteModel<out T>>
    ): BulkWriteResult = wrapped.bulkWrite(clientSession, requests)

    override fun bulkWrite(requests: MutableList<out WriteModel<out T>>, options: BulkWriteOptions): BulkWriteResult =
        wrapped.bulkWrite(requests, options)

    override fun bulkWrite(requests: MutableList<out WriteModel<out T>>): BulkWriteResult = wrapped.bulkWrite(requests)
}
