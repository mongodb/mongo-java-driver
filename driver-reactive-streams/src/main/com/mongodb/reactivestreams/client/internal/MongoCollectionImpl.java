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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.async.client.AsyncMongoCollection;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;


final class MongoCollectionImpl<TDocument> implements MongoCollection<TDocument> {

    private final AsyncMongoCollection<TDocument> wrapped;

    MongoCollectionImpl(final AsyncMongoCollection<TDocument> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    @Override
    public Class<TDocument> getDocumentClass() {
        return wrapped.getDocumentClass();
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return wrapped.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return wrapped.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }

    @Override
    public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(final Class<NewTDocument> clazz) {
        return new MongoCollectionImpl<>(wrapped.withDocumentClass(clazz));
    }

    @Override
    public MongoCollection<TDocument> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoCollectionImpl<>(wrapped.withCodecRegistry(codecRegistry));
    }

    @Override
    public MongoCollection<TDocument> withReadPreference(final ReadPreference readPreference) {
        return new MongoCollectionImpl<>(wrapped.withReadPreference(readPreference));
    }

    @Override
    public MongoCollection<TDocument> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionImpl<>(wrapped.withWriteConcern(writeConcern));
    }

    @Override
    public MongoCollection<TDocument> withReadConcern(final ReadConcern readConcern) {
        return new MongoCollectionImpl<>(wrapped.withReadConcern(readConcern));
    }

    @Override
    public Publisher<Long> estimatedDocumentCount() {
        return estimatedDocumentCount(new EstimatedDocumentCountOptions());
    }

    @Override
    public Publisher<Long> estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        return Publishers.publish(callback -> wrapped.estimatedDocumentCount(options, callback));
    }

    @Override
    public Publisher<Long> countDocuments() {
        return countDocuments(new BsonDocument());
    }

    @Override
    public Publisher<Long> countDocuments(final Bson filter) {
        return countDocuments(filter, new CountOptions());
    }

    @Override
    public Publisher<Long> countDocuments(final Bson filter, final CountOptions options) {
        return Publishers.publish(callback -> wrapped.countDocuments(filter, options, callback));
    }

    @Override
    public Publisher<Long> countDocuments(final ClientSession clientSession) {
        return countDocuments(clientSession, new BsonDocument());
    }

    @Override
    public Publisher<Long> countDocuments(final ClientSession clientSession, final Bson filter) {
        return countDocuments(clientSession, filter, new CountOptions());
    }

    @Override
    public Publisher<Long> countDocuments(final ClientSession clientSession, final Bson filter, final CountOptions options) {
        return Publishers.publish(
                callback -> wrapped.countDocuments(clientSession.getWrapped(), filter, options, callback));
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final String fieldName, final Class<TResult> resultClass) {
        return distinct(fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final String fieldName, final Bson filter, final Class<TResult> resultClass) {
        return new DistinctPublisherImpl<>(wrapped.distinct(fieldName, resultClass)).filter(filter);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final ClientSession clientSession, final String fieldName,
                                                         final Class<TResult> resultClass) {
        return distinct(clientSession, fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final ClientSession clientSession, final String fieldName, final Bson filter,
                                                         final Class<TResult> resultClass) {
        return new DistinctPublisherImpl<>(wrapped.distinct(clientSession.getWrapped(), fieldName, resultClass)).filter(filter);
    }

    @Override
    public FindPublisher<TDocument> find() {
        return find(new BsonDocument(), getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final Class<TResult> clazz) {
        return find(new BsonDocument(), clazz);
    }

    @Override
    public FindPublisher<TDocument> find(final Bson filter) {
        return find(filter, getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final Bson filter, final Class<TResult> clazz) {
        return new FindPublisherImpl<>(wrapped.find(filter, clazz));
    }

    @Override
    public FindPublisher<TDocument> find(final ClientSession clientSession) {
        return find(clientSession, new BsonDocument(), getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final ClientSession clientSession, final Class<TResult> clazz) {
        return find(clientSession, new BsonDocument(), clazz);
    }

    @Override
    public FindPublisher<TDocument> find(final ClientSession clientSession, final Bson filter) {
        return find(clientSession, filter, getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final ClientSession clientSession, final Bson filter, final Class<TResult> clazz) {
        return new FindPublisherImpl<>(wrapped.find(clientSession.getWrapped(), filter, clazz));
    }

    @Override
    public AggregatePublisher<TDocument> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, getDocumentClass());
    }

    @Override
    public <TResult> AggregatePublisher<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> clazz) {
        return new AggregatePublisherImpl<>(wrapped.aggregate(pipeline, clazz));
    }

    @Override
    public AggregatePublisher<TDocument> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, getDocumentClass());
    }

    @Override
    public <TResult> AggregatePublisher<TResult> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                           final Class<TResult> clazz) {
        return new AggregatePublisherImpl<>(wrapped.aggregate(clientSession.getWrapped(), pipeline, clazz));
    }

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return watch(Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new ChangeStreamPublisherImpl<>(wrapped.watch(pipeline, resultClass));
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                          final Class<TResult> resultClass) {
        return new ChangeStreamPublisherImpl<>(wrapped.watch(clientSession.getWrapped(), pipeline, resultClass));
    }

    @Override
    public MapReducePublisher<TDocument> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, getDocumentClass());
    }

    @Override
    public <TResult> MapReducePublisher<TResult> mapReduce(final String mapFunction, final String reduceFunction,
                                                           final Class<TResult> clazz) {
        return new MapReducePublisherImpl<>(wrapped.mapReduce(mapFunction, reduceFunction, clazz));
    }

    @Override
    public MapReducePublisher<TDocument> mapReduce(final ClientSession clientSession, final String mapFunction,
                                                  final String reduceFunction) {
        return mapReduce(clientSession, mapFunction, reduceFunction, getDocumentClass());
    }

    @Override
    public <TResult> MapReducePublisher<TResult> mapReduce(final ClientSession clientSession, final String mapFunction,
                                                           final String reduceFunction, final Class<TResult> clazz) {
        return new MapReducePublisherImpl<>(wrapped.mapReduce(clientSession.getWrapped(), mapFunction, reduceFunction, clazz));
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests,
                                                final BulkWriteOptions options) {
        return Publishers.publish(
                callback -> wrapped.bulkWrite(requests, options, callback));
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final ClientSession clientSession,
                                                final List<? extends WriteModel<? extends TDocument>> requests) {
        return bulkWrite(clientSession, requests, new BulkWriteOptions());
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final ClientSession clientSession,
                                                final List<? extends WriteModel<? extends TDocument>> requests,
                                                final BulkWriteOptions options) {
        return Publishers.publish(
                callback -> wrapped.bulkWrite(clientSession.getWrapped(), requests, options, callback));
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final TDocument document) {
        return insertOne(document, new InsertOneOptions());
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final TDocument document, final InsertOneOptions options) {
        return Publishers.publish(
                callback -> wrapped.insertOne(document, options, callback));
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final ClientSession clientSession, final TDocument document) {
        return insertOne(clientSession, document, new InsertOneOptions());
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final ClientSession clientSession, final TDocument document,
                                                final InsertOneOptions options) {
        return Publishers.publish(
                callback -> wrapped.insertOne(clientSession.getWrapped(), document, options, callback));
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final List<? extends TDocument> documents) {
        return insertMany(documents, new InsertManyOptions());
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final List<? extends TDocument> documents, final InsertManyOptions options) {
        return Publishers.publish(
                callback -> wrapped.insertMany(documents, options, callback));
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final ClientSession clientSession, final List<? extends TDocument> documents) {
        return insertMany(clientSession, documents, new InsertManyOptions());
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final ClientSession clientSession, final List<? extends TDocument> documents,
                                         final InsertManyOptions options) {
        return Publishers.publish(
                callback -> wrapped.insertMany(clientSession.getWrapped(), documents, options, callback));
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final Bson filter) {
        return deleteOne(filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        return Publishers.publish(
                callback -> wrapped.deleteOne(filter, options, callback));
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final ClientSession clientSession, final Bson filter) {
        return deleteOne(clientSession, filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        return Publishers.publish(
                callback -> wrapped.deleteOne(clientSession.getWrapped(), filter, options, callback));
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final Bson filter) {
        return deleteMany(filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        return Publishers.publish(
                callback -> wrapped.deleteMany(filter, options, callback));
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final ClientSession clientSession, final Bson filter) {
        return deleteMany(clientSession, filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        return Publishers.publish(
                callback -> wrapped.deleteMany(clientSession.getWrapped(), filter, options, callback));
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final Bson filter, final TDocument replacement) {
        return replaceOne(filter, replacement, new ReplaceOptions());
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final Bson filter, final TDocument replacement, final ReplaceOptions options) {
        return Publishers.publish(
                callback -> wrapped.replaceOne(filter, replacement, options, callback));
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final ClientSession clientSession, final Bson filter, final TDocument replacement) {
        return replaceOne(clientSession, filter, replacement, new ReplaceOptions());
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final ClientSession clientSession, final Bson filter, final TDocument replacement,
                                              final ReplaceOptions options) {
        return Publishers.publish(
                callback -> wrapped.replaceOne(clientSession.getWrapped(), filter, replacement, options, callback));
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final Bson update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final Bson update, final UpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.updateOne(filter, update, options, callback));
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final Bson update) {
        return updateOne(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final Bson update,
                                             final UpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.updateOne(clientSession.getWrapped(), filter, update, options, callback));
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final List<? extends Bson> update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final List<? extends Bson> update, final UpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.updateOne(filter, update, options, callback));
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return updateOne(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                             final UpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.updateOne(clientSession.getWrapped(), filter, update, options, callback));
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final Bson update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final Bson update, final UpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.updateMany(filter, update, options, callback));
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final Bson update) {
        return updateMany(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final Bson update,
                                              final UpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.updateMany(clientSession.getWrapped(), filter, update, options, callback));
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final List<? extends Bson> update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final List<? extends Bson> update, final UpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.updateMany(filter, update, options, callback));
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return updateMany(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                              final UpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.updateMany(clientSession.getWrapped(), filter, update, options, callback));
    }

    @Override
    public Publisher<TDocument> findOneAndDelete(final Bson filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    public Publisher<TDocument> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return Publishers.publish(
                callback -> wrapped.findOneAndDelete(filter, options, callback));
    }

    @Override
    public Publisher<TDocument> findOneAndDelete(final ClientSession clientSession, final Bson filter) {
        return findOneAndDelete(clientSession, filter, new FindOneAndDeleteOptions());
    }

    @Override
    public Publisher<TDocument> findOneAndDelete(final ClientSession clientSession, final Bson filter,
                                                 final FindOneAndDeleteOptions options) {
        return Publishers.publish(
                callback -> wrapped.findOneAndDelete(clientSession.getWrapped(), filter, options, callback));
    }

    @Override
    public Publisher<TDocument> findOneAndReplace(final Bson filter, final TDocument replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public Publisher<TDocument> findOneAndReplace(final Bson filter, final TDocument replacement, final FindOneAndReplaceOptions options) {
        return Publishers.publish(
                callback -> wrapped.findOneAndReplace(filter, replacement, options, callback));
    }

    @Override
    public Publisher<TDocument> findOneAndReplace(final ClientSession clientSession, final Bson filter, final TDocument replacement) {
        return findOneAndReplace(clientSession, filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public Publisher<TDocument> findOneAndReplace(final ClientSession clientSession, final Bson filter, final TDocument replacement,
                                                  final FindOneAndReplaceOptions options) {
        return Publishers.publish(
                callback -> wrapped.findOneAndReplace(clientSession.getWrapped(), filter, replacement, options, callback));
    }

    @Override
    public Publisher<TDocument> findOneAndUpdate(final Bson filter, final Bson update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<TDocument> findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.findOneAndUpdate(filter, update, options, callback));
    }

    @Override
    public Publisher<TDocument> findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<TDocument> findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update,
                                                 final FindOneAndUpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.findOneAndUpdate(clientSession.getWrapped(), filter, update, options, callback));
    }

    @Override
    public Publisher<TDocument> findOneAndUpdate(final Bson filter, final List<? extends Bson> update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<TDocument> findOneAndUpdate(final Bson filter, final List<? extends Bson> update,
                                                 final FindOneAndUpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.findOneAndUpdate(filter, update, options, callback));
    }

    @Override
    public Publisher<TDocument> findOneAndUpdate(final ClientSession clientSession, final Bson filter,
                                                 final List<? extends Bson> update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<TDocument> findOneAndUpdate(final ClientSession clientSession, final Bson filter,
                                                 final List<? extends Bson> update, final FindOneAndUpdateOptions options) {
        return Publishers.publish(
                callback -> wrapped.findOneAndUpdate(clientSession.getWrapped(), filter, update, options, callback));
    }

    @Override
    public Publisher<Void> drop() {
        return Publishers.publish(wrapped::drop);
    }

    @Override
    public Publisher<Void> drop(final ClientSession clientSession) {
        return Publishers.publish(
                callback -> wrapped.drop(clientSession.getWrapped(), callback));
    }

    @Override
    public Publisher<String> createIndex(final Bson key) {
        return createIndex(key, new IndexOptions());
    }

    @Override
    public Publisher<String> createIndex(final Bson key, final IndexOptions options) {
        return Publishers.publish(
                callback -> wrapped.createIndex(key, options, callback));
    }

    @Override
    public Publisher<String> createIndex(final ClientSession clientSession, final Bson key) {
        return createIndex(clientSession, key, new IndexOptions());
    }

    @Override
    public Publisher<String> createIndex(final ClientSession clientSession, final Bson key, final IndexOptions options) {
        return Publishers.publish(
                callback -> wrapped.createIndex(clientSession.getWrapped(), key, options, callback));
    }

    @Override
    public Publisher<String> createIndexes(final List<IndexModel> indexes) {
        return createIndexes(indexes, new CreateIndexOptions());
    }

    @Override
    public Publisher<String> createIndexes(final List<IndexModel> indexes, final CreateIndexOptions createIndexOptions) {
        return Publishers.publishAndFlatten(
                callback -> wrapped.createIndexes(indexes, createIndexOptions, callback));
    }

    @Override
    public Publisher<String> createIndexes(final ClientSession clientSession, final List<IndexModel> indexes) {
        return createIndexes(clientSession, indexes, new CreateIndexOptions());
    }

    @Override
    public Publisher<String> createIndexes(final ClientSession clientSession, final List<IndexModel> indexes,
                                           final CreateIndexOptions createIndexOptions) {
        return Publishers.publishAndFlatten(
                callback -> wrapped.createIndexes(clientSession.getWrapped(), indexes, createIndexOptions, callback));
    }

    @Override
    public ListIndexesPublisher<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <TResult> ListIndexesPublisher<TResult> listIndexes(final Class<TResult> clazz) {
        return new ListIndexesPublisherImpl<>(wrapped.listIndexes(clazz));
    }

    @Override
    public ListIndexesPublisher<Document> listIndexes(final ClientSession clientSession) {
        return listIndexes(clientSession, Document.class);
    }

    @Override
    public <TResult> ListIndexesPublisher<TResult> listIndexes(final ClientSession clientSession, final Class<TResult> clazz) {
        return new ListIndexesPublisherImpl<>(wrapped.listIndexes(clientSession.getWrapped(), clazz));
    }

    @Override
    public Publisher<Void> dropIndex(final String indexName) {
        return dropIndex(indexName, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndex(final Bson keys) {
        return dropIndex(keys, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndex(final String indexName, final DropIndexOptions dropIndexOptions) {
        return Publishers.publish(
                callback -> wrapped.dropIndex(indexName, dropIndexOptions, callback));
    }

    @Override
    public Publisher<Void> dropIndex(final Bson keys, final DropIndexOptions dropIndexOptions) {
        return Publishers.publish(
                callback -> wrapped.dropIndex(keys, dropIndexOptions, callback));
    }

    @Override
    public Publisher<Void> dropIndex(final ClientSession clientSession, final String indexName) {
        return dropIndex(clientSession, indexName, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndex(final ClientSession clientSession, final Bson keys) {
        return dropIndex(clientSession, keys, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndex(final ClientSession clientSession, final String indexName,
                                        final DropIndexOptions dropIndexOptions) {
        return Publishers.publish(
                callback -> wrapped.dropIndex(clientSession.getWrapped(), indexName, dropIndexOptions, callback));
    }

    @Override
    public Publisher<Void> dropIndex(final ClientSession clientSession, final Bson keys, final DropIndexOptions dropIndexOptions) {
        return Publishers.publish(
                callback -> wrapped.dropIndex(clientSession.getWrapped(), keys, dropIndexOptions, callback));
    }

    @Override
    public Publisher<Void> dropIndexes() {
        return dropIndexes(new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndexes(final DropIndexOptions dropIndexOptions) {
        return dropIndex("*", dropIndexOptions);
    }

    @Override
    public Publisher<Void> dropIndexes(final ClientSession clientSession) {
        return dropIndexes(clientSession, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndexes(final ClientSession clientSession, final DropIndexOptions dropIndexOptions) {
        return dropIndex(clientSession, "*", dropIndexOptions);
    }

    @Override
    public Publisher<Void> renameCollection(final MongoNamespace newCollectionNamespace) {
        return renameCollection(newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public Publisher<Void> renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions options) {
        return Publishers.publish(
                callback -> wrapped.renameCollection(newCollectionNamespace, options, callback));
    }

    @Override
    public Publisher<Void> renameCollection(final ClientSession clientSession, final MongoNamespace newCollectionNamespace) {
        return renameCollection(clientSession, newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public Publisher<Void> renameCollection(final ClientSession clientSession, final MongoNamespace newCollectionNamespace,
                                               final RenameCollectionOptions options) {
        return Publishers.publish(
                callback -> wrapped.renameCollection(clientSession.getWrapped(), newCollectionNamespace, options,
                        callback));
    }

}
