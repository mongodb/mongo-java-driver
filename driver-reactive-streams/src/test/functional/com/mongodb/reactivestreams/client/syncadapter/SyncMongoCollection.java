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
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

import static java.util.Objects.requireNonNull;

class SyncMongoCollection<T> implements MongoCollection<T> {

    private final com.mongodb.reactivestreams.client.MongoCollection<T> wrapped;

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
        SingleResultSubscriber<Long> subscriber = new SingleResultSubscriber<>();
        wrapped.countDocuments().subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public long countDocuments(final Bson filter) {
        SingleResultSubscriber<Long> subscriber = new SingleResultSubscriber<>();
        wrapped.countDocuments(filter).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public long countDocuments(final Bson filter, final CountOptions options) {
        SingleResultSubscriber<Long> subscriber = new SingleResultSubscriber<>();
        wrapped.countDocuments(filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public long countDocuments(final ClientSession clientSession) {
        SingleResultSubscriber<Long> subscriber = new SingleResultSubscriber<>();
        wrapped.countDocuments(unwrap(clientSession)).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public long countDocuments(final ClientSession clientSession, final Bson filter) {
        SingleResultSubscriber<Long> subscriber = new SingleResultSubscriber<>();
        wrapped.countDocuments(unwrap(clientSession), filter).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public long countDocuments(final ClientSession clientSession, final Bson filter, final CountOptions options) {
        SingleResultSubscriber<Long> subscriber = new SingleResultSubscriber<>();
        wrapped.countDocuments(unwrap(clientSession), filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public long estimatedDocumentCount() {
        SingleResultSubscriber<Long> subscriber = new SingleResultSubscriber<>();
        wrapped.estimatedDocumentCount().subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public long estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        SingleResultSubscriber<Long> subscriber = new SingleResultSubscriber<>();
        wrapped.estimatedDocumentCount(options).subscribe(subscriber);
        return requireNonNull(subscriber.get());
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
        return new SyncAggregateIterable<>(wrapped.aggregate(pipeline, wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new SyncAggregateIterable<>(wrapped.aggregate(pipeline, resultClass));
    }

    @Override
    public AggregateIterable<T> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return new SyncAggregateIterable<>(wrapped.aggregate(unwrap(clientSession), pipeline, wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                          final Class<TResult> resultClass) {
        return new SyncAggregateIterable<>(wrapped.aggregate(unwrap(clientSession), pipeline, resultClass));
    }

    @Override
    public ChangeStreamIterable<T> watch() {
        return new SyncChangeStreamIterable<>(wrapped.watch(wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(resultClass));
    }

    @Override
    public ChangeStreamIterable<T> watch(final List<? extends Bson> pipeline) {
        return new SyncChangeStreamIterable<>(wrapped.watch(wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(pipeline, resultClass));
    }

    @Override
    public ChangeStreamIterable<T> watch(final ClientSession clientSession) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), resultClass));
    }

    @Override
    public ChangeStreamIterable<T> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        // API inconsistency
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), pipeline, wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                         final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), pipeline, resultClass));
    }

    @Override
    public MapReduceIterable<T> mapReduce(final String mapFunction, final String reduceFunction) {
        return new SyncMapReduceIterable<>(wrapped.mapReduce(mapFunction, reduceFunction, wrapped.getDocumentClass()));
    }

    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(final String mapFunction, final String reduceFunction,
                                                          final Class<TResult> resultClass) {
        return new SyncMapReduceIterable<>(wrapped.mapReduce(mapFunction, reduceFunction, resultClass));
    }

    @Override
    public MapReduceIterable<T> mapReduce(final ClientSession clientSession, final String mapFunction, final String reduceFunction) {
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
        SingleResultSubscriber<BulkWriteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.bulkWrite(requests).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options) {
        SingleResultSubscriber<BulkWriteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.bulkWrite(requests, options).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public BulkWriteResult bulkWrite(final ClientSession clientSession, final List<? extends WriteModel<? extends T>> requests) {
        SingleResultSubscriber<BulkWriteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.bulkWrite(unwrap(clientSession), requests).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public BulkWriteResult bulkWrite(final ClientSession clientSession, final List<? extends WriteModel<? extends T>> requests,
                                     final BulkWriteOptions options) {
        SingleResultSubscriber<BulkWriteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.bulkWrite(unwrap(clientSession), requests, options).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public InsertOneResult insertOne(final T t) {
        SingleResultSubscriber<InsertOneResult> subscriber = new SingleResultSubscriber<>();
        wrapped.insertOne(t).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public InsertOneResult insertOne(final T t, final InsertOneOptions options) {
        SingleResultSubscriber<InsertOneResult> subscriber = new SingleResultSubscriber<>();
        wrapped.insertOne(t, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public InsertOneResult insertOne(final ClientSession clientSession, final T t) {
        SingleResultSubscriber<InsertOneResult> subscriber = new SingleResultSubscriber<>();
        wrapped.insertOne(unwrap(clientSession), t).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public InsertOneResult insertOne(final ClientSession clientSession, final T t, final InsertOneOptions options) {
        SingleResultSubscriber<InsertOneResult> subscriber = new SingleResultSubscriber<>();
        wrapped.insertOne(unwrap(clientSession), t, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public InsertManyResult insertMany(final List<? extends T> documents) {
        SingleResultSubscriber<InsertManyResult> subscriber = new SingleResultSubscriber<>();
        wrapped.insertMany(documents).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public InsertManyResult insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        SingleResultSubscriber<InsertManyResult> subscriber = new SingleResultSubscriber<>();
        wrapped.insertMany(documents, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public InsertManyResult insertMany(final ClientSession clientSession, final List<? extends T> documents) {
        SingleResultSubscriber<InsertManyResult> subscriber = new SingleResultSubscriber<>();
        wrapped.insertMany(unwrap(clientSession), documents).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public InsertManyResult insertMany(final ClientSession clientSession, final List<? extends T> documents,
                                       final InsertManyOptions options) {
        SingleResultSubscriber<InsertManyResult> subscriber = new SingleResultSubscriber<>();
        wrapped.insertMany(unwrap(clientSession), documents, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public DeleteResult deleteOne(final Bson filter) {
        SingleResultSubscriber<DeleteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.deleteOne(filter).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public DeleteResult deleteOne(final Bson filter, final DeleteOptions options) {
        SingleResultSubscriber<DeleteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.deleteOne(filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public DeleteResult deleteOne(final ClientSession clientSession, final Bson filter) {
        SingleResultSubscriber<DeleteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.deleteOne(unwrap(clientSession), filter).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public DeleteResult deleteOne(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        SingleResultSubscriber<DeleteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.deleteOne(unwrap(clientSession), filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public DeleteResult deleteMany(final Bson filter) {
        SingleResultSubscriber<DeleteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.deleteMany(filter).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public DeleteResult deleteMany(final Bson filter, final DeleteOptions options) {
        SingleResultSubscriber<DeleteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.deleteMany(filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public DeleteResult deleteMany(final ClientSession clientSession, final Bson filter) {
        SingleResultSubscriber<DeleteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.deleteMany(unwrap(clientSession), filter).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public DeleteResult deleteMany(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        SingleResultSubscriber<DeleteResult> subscriber = new SingleResultSubscriber<>();
        wrapped.deleteMany(unwrap(clientSession), filter, options).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult replaceOne(final Bson filter, final T replacement) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.replaceOne(filter, replacement).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult replaceOne(final Bson filter, final T replacement, final ReplaceOptions replaceOptions) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.replaceOne(filter, replacement, replaceOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult replaceOne(final ClientSession clientSession, final Bson filter, final T replacement) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.replaceOne(unwrap(clientSession), filter, replacement).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult replaceOne(final ClientSession clientSession, final Bson filter, final T replacement,
                                   final ReplaceOptions replaceOptions) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.replaceOne(unwrap(clientSession), filter, replacement, replaceOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final Bson update) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateOne(filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateOne(filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final Bson update) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateOne(unwrap(clientSession), filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final Bson update,
                                  final UpdateOptions updateOptions) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateOne(unwrap(clientSession), filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final List<? extends Bson> update) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateOne(filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final List<? extends Bson> update, final UpdateOptions updateOptions) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateOne(filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateOne(unwrap(clientSession), filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                  final UpdateOptions updateOptions) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateOne(unwrap(clientSession), filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final Bson update) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateMany(filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateMany(filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final Bson update) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateMany(unwrap(clientSession), filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final Bson update,
                                   final UpdateOptions updateOptions) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateMany(unwrap(clientSession), filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final List<? extends Bson> update) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateMany(filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final List<? extends Bson> update, final UpdateOptions updateOptions) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateMany(filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateMany(unwrap(clientSession), filter, update).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                   final UpdateOptions updateOptions) {
        SingleResultSubscriber<UpdateResult> subscriber = new SingleResultSubscriber<>();
        wrapped.updateMany(unwrap(clientSession), filter, update, updateOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public T findOneAndDelete(final Bson filter) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndDelete(filter).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndDelete(filter, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndDelete(final ClientSession clientSession, final Bson filter) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndDelete(unwrap(clientSession), filter).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndDelete(final ClientSession clientSession, final Bson filter, final FindOneAndDeleteOptions options) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndDelete(unwrap(clientSession), filter, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndReplace(final Bson filter, final T replacement) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndReplace(filter, replacement).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndReplace(final Bson filter, final T replacement, final FindOneAndReplaceOptions options) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndReplace(filter, replacement, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndReplace(final ClientSession clientSession, final Bson filter, final T replacement) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndReplace(unwrap(clientSession), filter, replacement).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndReplace(final ClientSession clientSession, final Bson filter, final T replacement,
                               final FindOneAndReplaceOptions options) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndReplace(unwrap(clientSession), filter, replacement, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final Bson update) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndUpdate(filter, update).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndUpdate(filter, update, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndUpdate(unwrap(clientSession), filter, update).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update,
                              final FindOneAndUpdateOptions options) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndUpdate(unwrap(clientSession), filter, update, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final List<? extends Bson> update) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndUpdate(filter, update).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final List<? extends Bson> update, final FindOneAndUpdateOptions options) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndUpdate(filter, update, options).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndUpdate(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndUpdate(unwrap(clientSession), filter, update).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public T findOneAndUpdate(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                              final FindOneAndUpdateOptions options) {
        SingleResultSubscriber<T> subscriber = new SingleResultSubscriber<>();
        wrapped.findOneAndUpdate(unwrap(clientSession), filter, update).subscribe(subscriber);
        return subscriber.get();
    }

    @Override
    public void drop() {
        SingleResultSubscriber<Void> subscriber = new SingleResultSubscriber<>();
        wrapped.drop().subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public void drop(final ClientSession clientSession) {
        SingleResultSubscriber<Void> subscriber = new SingleResultSubscriber<>();
        wrapped.drop(unwrap(clientSession)).subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public String createIndex(final Bson keys) {
        SingleResultSubscriber<String> subscriber = new SingleResultSubscriber<>();
        wrapped.createIndex(keys).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public String createIndex(final Bson keys, final IndexOptions indexOptions) {
        SingleResultSubscriber<String> subscriber = new SingleResultSubscriber<>();
        wrapped.createIndex(keys, indexOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public String createIndex(final ClientSession clientSession, final Bson keys) {
        SingleResultSubscriber<String> subscriber = new SingleResultSubscriber<>();
        wrapped.createIndex(unwrap(clientSession), keys).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public String createIndex(final ClientSession clientSession, final Bson keys, final IndexOptions indexOptions) {
        SingleResultSubscriber<String> subscriber = new SingleResultSubscriber<>();
        wrapped.createIndex(unwrap(clientSession), keys, indexOptions).subscribe(subscriber);
        return requireNonNull(subscriber.get());
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
        return new SyncListIndexesIterable<>(wrapped.listIndexes(resultClass));
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
        SingleResultSubscriber<Void> subscriber = new SingleResultSubscriber<>();
        wrapped.dropIndex(indexName).subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public void dropIndex(final String indexName, final DropIndexOptions dropIndexOptions) {
        SingleResultSubscriber<Void> subscriber = new SingleResultSubscriber<>();
        wrapped.dropIndex(indexName, dropIndexOptions).subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public void dropIndex(final Bson keys) {
        SingleResultSubscriber<Void> subscriber = new SingleResultSubscriber<>();
        wrapped.dropIndex(keys).subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public void dropIndex(final Bson keys, final DropIndexOptions dropIndexOptions) {
        SingleResultSubscriber<Void> subscriber = new SingleResultSubscriber<>();
        wrapped.dropIndex(keys, dropIndexOptions).subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final String indexName) {
        SingleResultSubscriber<Void> subscriber = new SingleResultSubscriber<>();
        wrapped.dropIndex(unwrap(clientSession), indexName).subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final Bson keys) {
        SingleResultSubscriber<Void> subscriber = new SingleResultSubscriber<>();
        wrapped.dropIndex(unwrap(clientSession), keys).subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final String indexName, final DropIndexOptions dropIndexOptions) {
        SingleResultSubscriber<Void> subscriber = new SingleResultSubscriber<>();
        wrapped.dropIndex(unwrap(clientSession), indexName, dropIndexOptions).subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final Bson keys, final DropIndexOptions dropIndexOptions) {
        SingleResultSubscriber<Void> subscriber = new SingleResultSubscriber<>();
        wrapped.dropIndex(unwrap(clientSession), keys, dropIndexOptions).subscribe(subscriber);
        subscriber.get();
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
