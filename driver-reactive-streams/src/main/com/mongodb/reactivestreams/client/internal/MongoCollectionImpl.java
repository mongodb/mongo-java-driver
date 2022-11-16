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
import com.mongodb.client.model.DropCollectionOptions;
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
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;


final class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoOperationPublisher<T> mongoOperationPublisher;

    MongoCollectionImpl(final MongoOperationPublisher<T> mongoOperationPublisher) {
        this.mongoOperationPublisher = notNull("mongoOperationPublisher", mongoOperationPublisher);
    }

    @Override
    public MongoNamespace getNamespace() {
        return assertNotNull(mongoOperationPublisher.getNamespace());
    }

    @Override
    public Class<T> getDocumentClass() {
        return mongoOperationPublisher.getDocumentClass();
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return mongoOperationPublisher.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return mongoOperationPublisher.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return mongoOperationPublisher.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return mongoOperationPublisher.getReadConcern();
    }

    MongoOperationPublisher<T> getPublisherHelper() {
        return mongoOperationPublisher;
    }

    @Override
    public <D> MongoCollection<D> withDocumentClass(final Class<D> newDocumentClass) {
        return new MongoCollectionImpl<>(mongoOperationPublisher.withDocumentClass(newDocumentClass));
    }

    @Override
    public MongoCollection<T> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoCollectionImpl<>(mongoOperationPublisher.withCodecRegistry(codecRegistry));
    }

    @Override
    public MongoCollection<T> withReadPreference(final ReadPreference readPreference) {
        return new MongoCollectionImpl<>(mongoOperationPublisher.withReadPreference(readPreference));
    }

    @Override
    public MongoCollection<T> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionImpl<>(mongoOperationPublisher.withWriteConcern(writeConcern));
    }

    @Override
    public MongoCollection<T> withReadConcern(final ReadConcern readConcern) {
        return new MongoCollectionImpl<>(mongoOperationPublisher.withReadConcern(readConcern));
    }

    @Override
    public Publisher<Long> estimatedDocumentCount() {
        return estimatedDocumentCount(new EstimatedDocumentCountOptions());
    }

    @Override
    public Publisher<Long> estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        return mongoOperationPublisher.estimatedDocumentCount(options);
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
        return mongoOperationPublisher.countDocuments(null, filter, options);
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
        return mongoOperationPublisher.countDocuments(notNull("clientSession", clientSession), filter, options);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final String fieldName, final Class<TResult> resultClass) {
        return distinct(fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final String fieldName, final Bson filter, final Class<TResult> resultClass) {
        return new DistinctPublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(resultClass), fieldName, filter);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final ClientSession clientSession, final String fieldName,
                                                         final Class<TResult> resultClass) {
        return distinct(clientSession, fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final ClientSession clientSession, final String fieldName, final Bson filter,
                                                         final Class<TResult> resultClass) {
        return new DistinctPublisherImpl<>(notNull("clientSession", clientSession),
                                           mongoOperationPublisher.withDocumentClass(resultClass), fieldName, filter);
    }

    @Override
    public FindPublisher<T> find() {
        return find(new BsonDocument(), getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final Class<TResult> resultClass) {
        return find(new BsonDocument(), resultClass);
    }

    @Override
    public FindPublisher<T> find(final Bson filter) {
        return find(filter, getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final Bson filter, final Class<TResult> resultClass) {
        return new FindPublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(resultClass), filter);
    }

    @Override
    public FindPublisher<T> find(final ClientSession clientSession) {
        return find(clientSession, new BsonDocument(), getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final ClientSession clientSession, final Class<TResult> resultClass) {
        return find(clientSession, new BsonDocument(), resultClass);
    }

    @Override
    public FindPublisher<T> find(final ClientSession clientSession, final Bson filter) {
        return find(clientSession, filter, getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final ClientSession clientSession, final Bson filter, final Class<TResult> resultClass) {
        return new FindPublisherImpl<>(notNull("clientSession", clientSession),
                                       mongoOperationPublisher.withDocumentClass(resultClass), filter);
    }

    @Override
    public AggregatePublisher<T> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, getDocumentClass());
    }

    @Override
    public <TResult> AggregatePublisher<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new AggregatePublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(resultClass), pipeline,
                                            AggregationLevel.COLLECTION);
    }

    @Override
    public AggregatePublisher<T> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, getDocumentClass());
    }

    @Override
    public <TResult> AggregatePublisher<TResult> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                           final Class<TResult> resultClass) {
        return new AggregatePublisherImpl<>(notNull("clientSession", clientSession),
                                            mongoOperationPublisher.withDocumentClass(resultClass), pipeline, AggregationLevel.COLLECTION);
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
        return new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher, resultClass, pipeline,
                                               ChangeStreamLevel.COLLECTION);
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
        return new ChangeStreamPublisherImpl<>(notNull("clientSession", clientSession), mongoOperationPublisher, resultClass,
                                               pipeline, ChangeStreamLevel.COLLECTION);
    }

    @SuppressWarnings("deprecation")
    @Override
    public com.mongodb.reactivestreams.client.MapReducePublisher<T> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, getDocumentClass());
    }

    @SuppressWarnings("deprecation")
    @Override
    public <TResult> com.mongodb.reactivestreams.client.MapReducePublisher<TResult> mapReduce(final String mapFunction,
            final String reduceFunction, final Class<TResult> resultClass) {
        return new MapReducePublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(resultClass), mapFunction,
                                            reduceFunction);
    }

    @SuppressWarnings("deprecation")
    @Override
    public com.mongodb.reactivestreams.client.MapReducePublisher<T> mapReduce(final ClientSession clientSession, final String mapFunction,
                                           final String reduceFunction) {
        return mapReduce(clientSession, mapFunction, reduceFunction, getDocumentClass());
    }

    @SuppressWarnings("deprecation")
    @Override
    public <TResult> com.mongodb.reactivestreams.client.MapReducePublisher<TResult> mapReduce(final ClientSession clientSession,
            final String mapFunction, final String reduceFunction, final Class<TResult> resultClass) {
        return new MapReducePublisherImpl<>(notNull("clientSession", clientSession),
                                            mongoOperationPublisher.withDocumentClass(resultClass), mapFunction, reduceFunction);
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends T>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends T>> requests,
                                                final BulkWriteOptions options) {
        return mongoOperationPublisher.bulkWrite(null, requests, options);
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final ClientSession clientSession,
                                                final List<? extends WriteModel<? extends T>> requests) {
        return bulkWrite(clientSession, requests, new BulkWriteOptions());
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final ClientSession clientSession,
                                                final List<? extends WriteModel<? extends T>> requests,
                                                final BulkWriteOptions options) {
        return mongoOperationPublisher.bulkWrite(notNull("clientSession", clientSession), requests, options);
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final T document) {
        return insertOne(document, new InsertOneOptions());
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final T document, final InsertOneOptions options) {
        return mongoOperationPublisher.insertOne(null, document, options);
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final ClientSession clientSession, final T document) {
        return insertOne(clientSession, document, new InsertOneOptions());
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final ClientSession clientSession, final T document,
                                                final InsertOneOptions options) {
        return mongoOperationPublisher.insertOne(notNull("clientSession", clientSession), document, options);
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final List<? extends T> documents) {
        return insertMany(documents, new InsertManyOptions());
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        return mongoOperationPublisher.insertMany(null, documents, options);
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final ClientSession clientSession, final List<? extends T> documents) {
        return insertMany(clientSession, documents, new InsertManyOptions());
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final ClientSession clientSession, final List<? extends T> documents,
                                                  final InsertManyOptions options) {
        return mongoOperationPublisher.insertMany(notNull("clientSession", clientSession), documents, options);
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final Bson filter) {
        return deleteOne(filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        return mongoOperationPublisher.deleteOne(null, filter, options);
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final ClientSession clientSession, final Bson filter) {
        return deleteOne(clientSession, filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        return mongoOperationPublisher.deleteOne(notNull("clientSession", clientSession), filter, options);
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final Bson filter) {
        return deleteMany(filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        return mongoOperationPublisher.deleteMany(null, filter, options);
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final ClientSession clientSession, final Bson filter) {
        return deleteMany(clientSession, filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        return mongoOperationPublisher.deleteMany(notNull("clientSession", clientSession), filter, options);
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final Bson filter, final T replacement) {
        return replaceOne(filter, replacement, new ReplaceOptions());
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final Bson filter, final T replacement, final ReplaceOptions options) {
        return mongoOperationPublisher.replaceOne(null, filter, replacement, options);
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final ClientSession clientSession, final Bson filter, final T replacement) {
        return replaceOne(clientSession, filter, replacement, new ReplaceOptions());
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final ClientSession clientSession, final Bson filter, final T replacement,
                                              final ReplaceOptions options) {
        return mongoOperationPublisher.replaceOne(notNull("clientSession", clientSession), filter, replacement, options);
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final Bson update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final Bson update, final UpdateOptions options) {
        return mongoOperationPublisher.updateOne(null, filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final Bson update) {
        return updateOne(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final Bson update,
                                             final UpdateOptions options) {
        return mongoOperationPublisher.updateOne(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final List<? extends Bson> update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final List<? extends Bson> update, final UpdateOptions options) {
        return mongoOperationPublisher.updateOne(null, filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return updateOne(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                             final UpdateOptions options) {
        return mongoOperationPublisher.updateOne(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final Bson update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final Bson update, final UpdateOptions options) {
        return mongoOperationPublisher.updateMany(null, filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final Bson update) {
        return updateMany(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final Bson update,
                                              final UpdateOptions options) {
        return mongoOperationPublisher.updateMany(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final List<? extends Bson> update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final List<? extends Bson> update, final UpdateOptions options) {
        return mongoOperationPublisher.updateMany(null, filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return updateMany(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                              final UpdateOptions options) {
        return mongoOperationPublisher.updateMany(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<T> findOneAndDelete(final Bson filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    public Publisher<T> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return mongoOperationPublisher.findOneAndDelete(null, filter, options);
    }

    @Override
    public Publisher<T> findOneAndDelete(final ClientSession clientSession, final Bson filter) {
        return findOneAndDelete(clientSession, filter, new FindOneAndDeleteOptions());
    }

    @Override
    public Publisher<T> findOneAndDelete(final ClientSession clientSession, final Bson filter,
                                         final FindOneAndDeleteOptions options) {
        return mongoOperationPublisher.findOneAndDelete(notNull("clientSession", clientSession), filter, options);
    }

    @Override
    public Publisher<T> findOneAndReplace(final Bson filter, final T replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public Publisher<T> findOneAndReplace(final Bson filter, final T replacement, final FindOneAndReplaceOptions options) {
        return mongoOperationPublisher.findOneAndReplace(null, filter, replacement, options);
    }

    @Override
    public Publisher<T> findOneAndReplace(final ClientSession clientSession, final Bson filter, final T replacement) {
        return findOneAndReplace(clientSession, filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public Publisher<T> findOneAndReplace(final ClientSession clientSession, final Bson filter, final T replacement,
                                          final FindOneAndReplaceOptions options) {
        return mongoOperationPublisher.findOneAndReplace(notNull("clientSession", clientSession), filter, replacement, options);
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Bson filter, final Bson update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        return mongoOperationPublisher.findOneAndUpdate(null, filter, update, options);
    }

    @Override
    public Publisher<T> findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<T> findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update,
                                         final FindOneAndUpdateOptions options) {
        return mongoOperationPublisher.findOneAndUpdate(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Bson filter, final List<? extends Bson> update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Bson filter, final List<? extends Bson> update,
                                         final FindOneAndUpdateOptions options) {
        return mongoOperationPublisher.findOneAndUpdate(null, filter, update, options);
    }

    @Override
    public Publisher<T> findOneAndUpdate(final ClientSession clientSession, final Bson filter,
                                         final List<? extends Bson> update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<T> findOneAndUpdate(final ClientSession clientSession, final Bson filter,
                                         final List<? extends Bson> update, final FindOneAndUpdateOptions options) {
        return mongoOperationPublisher.findOneAndUpdate(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<Void> drop() {
        return mongoOperationPublisher.dropCollection(null, new DropCollectionOptions());
    }

    @Override
    public Publisher<Void> drop(final ClientSession clientSession) {
        return mongoOperationPublisher.dropCollection(notNull("clientSession", clientSession), new DropCollectionOptions());
    }

    @Override
    public Publisher<Void> drop(final DropCollectionOptions dropCollectionOptions) {
        return mongoOperationPublisher.dropCollection(null, dropCollectionOptions);
    }

    @Override
    public Publisher<Void> drop(final ClientSession clientSession, final DropCollectionOptions dropCollectionOptions) {
        return mongoOperationPublisher.dropCollection(notNull("clientSession", clientSession), dropCollectionOptions);
    }

    @Override
    public Publisher<String> createIndex(final Bson key) {
        return createIndex(key, new IndexOptions());
    }

    @Override
    public Publisher<String> createIndex(final Bson key, final IndexOptions options) {
        return mongoOperationPublisher.createIndex(null, key, options);
    }

    @Override
    public Publisher<String> createIndex(final ClientSession clientSession, final Bson key) {
        return createIndex(clientSession, key, new IndexOptions());
    }

    @Override
    public Publisher<String> createIndex(final ClientSession clientSession, final Bson key, final IndexOptions options) {
        return mongoOperationPublisher.createIndex(notNull("clientSession", clientSession), key, options);
    }

    @Override
    public Publisher<String> createIndexes(final List<IndexModel> indexes) {
        return createIndexes(indexes, new CreateIndexOptions());
    }

    @Override
    public Publisher<String> createIndexes(final List<IndexModel> indexes, final CreateIndexOptions options) {
        return mongoOperationPublisher.createIndexes(null, indexes, options);
    }

    @Override
    public Publisher<String> createIndexes(final ClientSession clientSession, final List<IndexModel> indexes) {
        return createIndexes(clientSession, indexes, new CreateIndexOptions());
    }

    @Override
    public Publisher<String> createIndexes(final ClientSession clientSession, final List<IndexModel> indexes,
            final CreateIndexOptions options) {
        return mongoOperationPublisher.createIndexes(notNull("clientSession", clientSession), indexes, options);
    }

    @Override
    public ListIndexesPublisher<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <TResult> ListIndexesPublisher<TResult> listIndexes(final Class<TResult> resultClass) {
        return new ListIndexesPublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(resultClass));
    }

    @Override
    public ListIndexesPublisher<Document> listIndexes(final ClientSession clientSession) {
        return listIndexes(clientSession, Document.class);
    }

    @Override
    public <TResult> ListIndexesPublisher<TResult> listIndexes(final ClientSession clientSession, final Class<TResult> resultClass) {
        return new ListIndexesPublisherImpl<>(notNull("clientSession", clientSession),
                                              mongoOperationPublisher.withDocumentClass(resultClass));
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
        return mongoOperationPublisher.dropIndex(null, indexName, dropIndexOptions);
    }

    @Override
    public Publisher<Void> dropIndex(final Bson keys, final DropIndexOptions dropIndexOptions) {
        return mongoOperationPublisher.dropIndex(null, keys, dropIndexOptions);
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
                                     final DropIndexOptions options) {
        return mongoOperationPublisher.dropIndex(notNull("clientSession", clientSession), indexName, options);
    }

    @Override
    public Publisher<Void> dropIndex(final ClientSession clientSession, final Bson keys, final DropIndexOptions options) {
        return mongoOperationPublisher.dropIndex(notNull("clientSession", clientSession), keys, options);
    }

    @Override
    public Publisher<Void> dropIndexes() {
        return dropIndexes(new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndexes(final DropIndexOptions options) {
        return mongoOperationPublisher.dropIndexes(null, options);
    }

    @Override
    public Publisher<Void> dropIndexes(final ClientSession clientSession) {
        return dropIndexes(clientSession, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndexes(final ClientSession clientSession, final DropIndexOptions options) {
        return mongoOperationPublisher.dropIndexes(notNull("clientSession", clientSession), options);
    }

    @Override
    public Publisher<Void> renameCollection(final MongoNamespace newCollectionNamespace) {
        return renameCollection(newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public Publisher<Void> renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions options) {
        return mongoOperationPublisher.renameCollection(null, newCollectionNamespace, options);
    }

    @Override
    public Publisher<Void> renameCollection(final ClientSession clientSession, final MongoNamespace newCollectionNamespace) {
        return renameCollection(clientSession, newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public Publisher<Void> renameCollection(final ClientSession clientSession, final MongoNamespace newCollectionNamespace,
                                            final RenameCollectionOptions options) {
        return mongoOperationPublisher.renameCollection(notNull("clientSession", clientSession), newCollectionNamespace, options);
    }

}
