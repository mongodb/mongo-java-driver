/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
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
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.Success;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SyncMongoCollection<T> implements MongoCollection<T> {

    private com.mongodb.reactivestreams.client.MongoCollection<T> wrapped;

    SyncMongoCollection(final com.mongodb.reactivestreams.client.MongoCollection<T> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    @Override
    public Class<T> getDocumentClass() {
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
        return new SyncMongoCollection<>(wrapped.withDocumentClass(clazz));
    }

    @Override
    public MongoCollection<T> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new SyncMongoCollection<>(wrapped.withCodecRegistry(codecRegistry));
    }

    @Override
    public MongoCollection<T> withReadPreference(final ReadPreference readPreference) {
        return new SyncMongoCollection<>(wrapped.withReadPreference(readPreference));
    }

    @Override
    public MongoCollection<T> withWriteConcern(final WriteConcern writeConcern) {
        return new SyncMongoCollection<>(wrapped.withWriteConcern(writeConcern));
    }

    @Override
    public MongoCollection<T> withReadConcern(final ReadConcern readConcern) {
        return new SyncMongoCollection<>(wrapped.withReadConcern(readConcern));
    }

    @Override
    public long countDocuments() {
        SyncSubscriber<Long> subscriber = new SyncSubscriber<>();
        wrapped.countDocuments().subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public long countDocuments(final Bson filter) {
        SyncSubscriber<Long> subscriber = new SyncSubscriber<>();
        wrapped.countDocuments(filter).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public long countDocuments(final Bson filter, final CountOptions options) {
        SyncSubscriber<Long> subscriber = new SyncSubscriber<>();
        wrapped.countDocuments(filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public long countDocuments(final ClientSession clientSession) {
        SyncSubscriber<Long> subscriber = new SyncSubscriber<>();
        wrapped.countDocuments(unwrap(clientSession)).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public long countDocuments(final ClientSession clientSession, final Bson filter) {
        SyncSubscriber<Long> subscriber = new SyncSubscriber<>();
        wrapped.countDocuments(unwrap(clientSession), filter).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public long countDocuments(final ClientSession clientSession, final Bson filter, final CountOptions options) {
        SyncSubscriber<Long> subscriber = new SyncSubscriber<>();
        wrapped.countDocuments(unwrap(clientSession), filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public long estimatedDocumentCount() {
        SyncSubscriber<Long> subscriber = new SyncSubscriber<>();
        wrapped.estimatedDocumentCount().subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public long estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        SyncSubscriber<Long> subscriber = new SyncSubscriber<>();
        wrapped.estimatedDocumentCount(options).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(final String fieldName, final Class<TResult> resultClass) {
        return new SyncDistinctIterable<>(wrapped.distinct(fieldName, resultClass));
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(final String fieldName, final Bson filter, final Class<TResult> resultClass) {
        return new SyncDistinctIterable<>(wrapped.distinct(fieldName, filter, resultClass));
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(final ClientSession clientSession, final String fieldName,
                                                        final Class<TResult> resultClass) {
        return new SyncDistinctIterable<>(wrapped.distinct(unwrap(clientSession), fieldName, resultClass));
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(final ClientSession clientSession, final String fieldName, final Bson filter,
                                                        final Class<TResult> resultClass) {
        return new SyncDistinctIterable<>(wrapped.distinct(unwrap(clientSession), fieldName, filter, resultClass));
    }

    @Override
    public FindIterable<T> find() {
        return new SyncFindIterable<>(wrapped.find());
    }

    @Override
    public <TResult> FindIterable<TResult> find(final Class<TResult> resultClass) {
        return new SyncFindIterable<>(wrapped.find(resultClass));
    }

    @Override
    public FindIterable<T> find(final Bson filter) {
        return new SyncFindIterable<>(wrapped.find(filter));
    }

    @Override
    public <TResult> FindIterable<TResult> find(final Bson filter, final Class<TResult> resultClass) {
        return new SyncFindIterable<>(wrapped.find(filter, resultClass));
    }

    @Override
    public FindIterable<T> find(final ClientSession clientSession) {
        return new SyncFindIterable<>(wrapped.find(unwrap(clientSession)));
    }

    @Override
    public <TResult> FindIterable<TResult> find(final ClientSession clientSession, final Class<TResult> resultClass) {
        return new SyncFindIterable<>(wrapped.find(unwrap(clientSession), resultClass));
    }

    @Override
    public FindIterable<T> find(final ClientSession clientSession, final Bson filter) {
        return new SyncFindIterable<>(wrapped.find(unwrap(clientSession), filter));
    }

    @Override
    public <TResult> FindIterable<TResult> find(final ClientSession clientSession, final Bson filter, final Class<TResult> resultClass) {
        return new SyncFindIterable<>(wrapped.find(unwrap(clientSession), filter, resultClass));
    }

    @Override
    public AggregateIterable<T> aggregate(final List<? extends Bson> pipeline) {
        // TODO: mismatch between sync and async APIs
        return new SyncAggregateIterable<>(wrapped.aggregate(pipeline, wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new SyncAggregateIterable<>(wrapped.aggregate(pipeline, resultClass));
    }

    @Override
    public AggregateIterable<T> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        // TODO: mismatch between sync and async APIs
        return new SyncAggregateIterable<>(wrapped.aggregate(unwrap(clientSession), pipeline, wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                          final Class<TResult> resultClass) {
        return new SyncAggregateIterable<>(wrapped.aggregate(unwrap(clientSession), pipeline, resultClass));
    }

    @Override
    public ChangeStreamIterable<T> watch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChangeStreamIterable<T> watch(final List<? extends Bson> pipeline) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChangeStreamIterable<T> watch(final ClientSession clientSession) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChangeStreamIterable<T> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                         final Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MapReduceIterable<T> mapReduce(final String mapFunction, final String reduceFunction) {
        // TODO: mismatch between sync and async APIs
        return new SyncMapReduceIterable<>(wrapped.mapReduce(mapFunction, reduceFunction, wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(final String mapFunction, final String reduceFunction,
                                                          final Class<TResult> resultClass) {
        return new SyncMapReduceIterable<>(wrapped.mapReduce(mapFunction, reduceFunction, resultClass));
    }

    @Override
    public MapReduceIterable<T> mapReduce(final ClientSession clientSession, final String mapFunction, final String reduceFunction) {
        // TODO: mismatch between sync and async APIs
        return new SyncMapReduceIterable<>(wrapped.mapReduce(unwrap(clientSession), mapFunction, reduceFunction,
                wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(final ClientSession clientSession, final String mapFunction,
                                                          final String reduceFunction, final Class<TResult> resultClass) {
        return new SyncMapReduceIterable<>(wrapped.mapReduce(unwrap(clientSession), mapFunction, reduceFunction, resultClass));
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests) {
        SyncSubscriber<BulkWriteResult> subscriber = new SyncSubscriber<>();
        wrapped.bulkWrite(requests).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options) {
        SyncSubscriber<BulkWriteResult> subscriber = new SyncSubscriber<>();
        wrapped.bulkWrite(requests, options).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public BulkWriteResult bulkWrite(final ClientSession clientSession, final List<? extends WriteModel<? extends T>> requests) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulkWriteResult bulkWrite(final ClientSession clientSession, final List<? extends WriteModel<? extends T>> requests,
                                     final BulkWriteOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertOne(final T t) {
        SyncSubscriber<Success> subscriber = new SyncSubscriber<>();
        wrapped.insertOne(t).subscribe(subscriber);
        subscriber.first();
    }

    @Override
    public void insertOne(final T t, final InsertOneOptions options) {
        SyncSubscriber<Success> subscriber = new SyncSubscriber<>();
        wrapped.insertOne(t, options).subscribe(subscriber);
        subscriber.first();
    }

    @Override
    public void insertOne(final ClientSession clientSession, final T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertOne(final ClientSession clientSession, final T t, final InsertOneOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertMany(final List<? extends T> documents) {
        SyncSubscriber<Success> subscriber = new SyncSubscriber<>();
        wrapped.insertMany(documents).subscribe(subscriber);
        subscriber.first();
    }

    @Override
    public void insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        SyncSubscriber<Success> subscriber = new SyncSubscriber<>();
        wrapped.insertMany(documents, options).subscribe(subscriber);
        subscriber.first();
    }

    @Override
    public void insertMany(final ClientSession clientSession, final List<? extends T> ts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertMany(final ClientSession clientSession, final List<? extends T> ts, final InsertManyOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteOne(final Bson filter) {
        SyncSubscriber<DeleteResult> subscriber = new SyncSubscriber<>();
        wrapped.deleteOne(filter).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public DeleteResult deleteOne(final Bson filter, final DeleteOptions options) {
        SyncSubscriber<DeleteResult> subscriber = new SyncSubscriber<>();
        wrapped.deleteOne(filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public DeleteResult deleteOne(final ClientSession clientSession, final Bson filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteOne(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteMany(final Bson filter) {
        SyncSubscriber<DeleteResult> subscriber = new SyncSubscriber<>();
        wrapped.deleteMany(filter).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public DeleteResult deleteMany(final Bson filter, final DeleteOptions options) {
        SyncSubscriber<DeleteResult> subscriber = new SyncSubscriber<>();
        wrapped.deleteMany(filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public DeleteResult deleteMany(final ClientSession clientSession, final Bson filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteMany(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult replaceOne(final Bson filter, final T replacement) {
        SyncSubscriber<UpdateResult> subscriber = new SyncSubscriber<>();
        wrapped.replaceOne(filter, replacement).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public UpdateResult replaceOne(final Bson filter, final T replacement, final ReplaceOptions replaceOptions) {
        SyncSubscriber<UpdateResult> subscriber = new SyncSubscriber<>();
        wrapped.replaceOne(filter, replacement, replaceOptions).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public UpdateResult replaceOne(final ClientSession clientSession, final Bson filter, final T replacement) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult replaceOne(final ClientSession clientSession, final Bson filter, final T replacement,
                                   final ReplaceOptions replaceOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final Bson update) {
        SyncSubscriber<UpdateResult> subscriber = new SyncSubscriber<>();
        wrapped.updateOne(filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        SyncSubscriber<UpdateResult> subscriber = new SyncSubscriber<>();
        wrapped.updateOne(filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final Bson update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final Bson update,
                                  final UpdateOptions updateOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final List<? extends Bson> update) {
        SyncSubscriber<UpdateResult> subscriber = new SyncSubscriber<>();
        wrapped.updateOne(filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final List<? extends Bson> update, final UpdateOptions updateOptions) {
        SyncSubscriber<UpdateResult> subscriber = new SyncSubscriber<>();
        wrapped.updateOne(filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                  final UpdateOptions updateOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final Bson update) {
        SyncSubscriber<UpdateResult> subscriber = new SyncSubscriber<>();
        wrapped.updateMany(filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        SyncSubscriber<UpdateResult> subscriber = new SyncSubscriber<>();
        wrapped.updateMany(filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final Bson update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final Bson update,
                                   final UpdateOptions updateOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final List<? extends Bson> update) {
        SyncSubscriber<UpdateResult> subscriber = new SyncSubscriber<>();
        wrapped.updateMany(filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final List<? extends Bson> update, final UpdateOptions updateOptions) {
        SyncSubscriber<UpdateResult> subscriber = new SyncSubscriber<>();
        wrapped.updateMany(filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                   final UpdateOptions updateOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T findOneAndDelete(final Bson filter) {
        SyncSubscriber<T> subscriber = new SyncSubscriber<>();
        wrapped.findOneAndDelete(filter).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public T findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        SyncSubscriber<T> subscriber = new SyncSubscriber<>();
        wrapped.findOneAndDelete(filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public T findOneAndDelete(final ClientSession clientSession, final Bson filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T findOneAndDelete(final ClientSession clientSession, final Bson filter, final FindOneAndDeleteOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T findOneAndReplace(final Bson filter, final T replacement) {
        SyncSubscriber<T> subscriber = new SyncSubscriber<>();
        wrapped.findOneAndReplace(filter, replacement).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public T findOneAndReplace(final Bson filter, final T replacement, final FindOneAndReplaceOptions options) {
        SyncSubscriber<T> subscriber = new SyncSubscriber<>();
        wrapped.findOneAndReplace(filter, replacement, options).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public T findOneAndReplace(final ClientSession clientSession, final Bson filter, final T replacement) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T findOneAndReplace(final ClientSession clientSession, final Bson filter, final T replacement,
                               final FindOneAndReplaceOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final Bson update) {
        SyncSubscriber<T> subscriber = new SyncSubscriber<>();
        wrapped.findOneAndUpdate(filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        SyncSubscriber<T> subscriber = new SyncSubscriber<>();
        wrapped.findOneAndUpdate(filter, update, options).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public T findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update,
                              final FindOneAndUpdateOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final List<? extends Bson> update) {
        SyncSubscriber<T> subscriber = new SyncSubscriber<>();
        wrapped.findOneAndUpdate(filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final List<? extends Bson> update, final FindOneAndUpdateOptions options) {
        SyncSubscriber<T> subscriber = new SyncSubscriber<>();
        wrapped.findOneAndUpdate(filter, update, options).subscribe(subscriber);
        return requireNonNull(subscriber.first());
    }

    @Override
    public T findOneAndUpdate(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T findOneAndUpdate(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                              final FindOneAndUpdateOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void drop() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void drop(final ClientSession clientSession) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createIndex(final Bson keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createIndex(final Bson keys, final IndexOptions indexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createIndex(final ClientSession clientSession, final Bson keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createIndex(final ClientSession clientSession, final Bson keys, final IndexOptions indexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> createIndexes(final List<IndexModel> indexes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> createIndexes(final List<IndexModel> indexes, final CreateIndexOptions createIndexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> createIndexes(final ClientSession clientSession, final List<IndexModel> indexes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> createIndexes(final ClientSession clientSession, final List<IndexModel> indexes,
                                      final CreateIndexOptions createIndexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIndexesIterable<Document> listIndexes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(final Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIndexesIterable<Document> listIndexes(final ClientSession clientSession) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(final ClientSession clientSession, final Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(final String indexName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(final String indexName, final DropIndexOptions dropIndexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(final Bson keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(final Bson keys, final DropIndexOptions dropIndexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final String indexName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final Bson keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final String indexName, final DropIndexOptions dropIndexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final Bson keys, final DropIndexOptions dropIndexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndexes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndexes(final ClientSession clientSession) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndexes(final DropIndexOptions dropIndexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndexes(final ClientSession clientSession, final DropIndexOptions dropIndexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions renameCollectionOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameCollection(final ClientSession clientSession, final MongoNamespace newCollectionNamespace) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameCollection(final ClientSession clientSession, final MongoNamespace newCollectionNamespace,
                                 final RenameCollectionOptions renameCollectionOptions) {
        throw new UnsupportedOperationException();
    }

    private com.mongodb.reactivestreams.client.ClientSession unwrap(final ClientSession clientSession) {
        return ((SyncClientSession) clientSession).getWrapped();
    }
}
