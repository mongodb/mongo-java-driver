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
import reactor.core.publisher.Mono;

import java.util.List;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
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
        return requireNonNull(Mono.from(wrapped.countDocuments()).block(TIMEOUT_DURATION));
    }

    @Override
    public long countDocuments(final Bson filter) {
        return requireNonNull(Mono.from(wrapped.countDocuments(filter)).block(TIMEOUT_DURATION));
    }

    @Override
    public long countDocuments(final Bson filter, final CountOptions options) {
        return requireNonNull(Mono.from(wrapped.countDocuments(filter, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public long countDocuments(final ClientSession clientSession) {
        return requireNonNull(Mono.from(wrapped.countDocuments(unwrap(clientSession))).block(TIMEOUT_DURATION));
    }

    @Override
    public long countDocuments(final ClientSession clientSession, final Bson filter) {
        return requireNonNull(Mono.from(wrapped.countDocuments(unwrap(clientSession), filter)).block(TIMEOUT_DURATION));
    }

    @Override
    public long countDocuments(final ClientSession clientSession, final Bson filter, final CountOptions options) {
        return requireNonNull(Mono.from(wrapped.countDocuments(unwrap(clientSession), filter, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public long estimatedDocumentCount() {
        return requireNonNull(Mono.from(wrapped.estimatedDocumentCount()).block(TIMEOUT_DURATION));
    }

    @Override
    public long estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        return requireNonNull(Mono.from(wrapped.estimatedDocumentCount(options)).block(TIMEOUT_DURATION));
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
    public <TResult> DistinctIterable<TResult> distinct(
            final ClientSession clientSession, final String fieldName,
            final Class<TResult> resultClass) {
        return new SyncDistinctIterable<>(wrapped.distinct(unwrap(clientSession), fieldName, resultClass));
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(
            final ClientSession clientSession, final String fieldName, final Bson filter,
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
    public <TResult> AggregateIterable<TResult> aggregate(
            final ClientSession clientSession, final List<? extends Bson> pipeline,
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
    public <TResult> ChangeStreamIterable<TResult> watch(
            final ClientSession clientSession, final List<? extends Bson> pipeline,
            final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), pipeline, resultClass));
    }

    @Override
    @SuppressWarnings("deprecation")
    public com.mongodb.client.MapReduceIterable<T> mapReduce(final String mapFunction, final String reduceFunction) {
        return new SyncMapReduceIterable<>(wrapped.mapReduce(mapFunction, reduceFunction, wrapped.getDocumentClass()));
    }

    @Override
    @SuppressWarnings("deprecation")
    public <TResult> com.mongodb.client.MapReduceIterable<TResult> mapReduce(
            final String mapFunction, final String reduceFunction,
            final Class<TResult> resultClass) {
        return new SyncMapReduceIterable<>(wrapped.mapReduce(mapFunction, reduceFunction, resultClass));
    }

    @Override
    @SuppressWarnings("deprecation")
    public com.mongodb.client.MapReduceIterable<T> mapReduce(final ClientSession clientSession, final String mapFunction,
            final String reduceFunction) {
        return new SyncMapReduceIterable<>(wrapped.mapReduce(unwrap(clientSession), mapFunction, reduceFunction,
                                                             wrapped.getDocumentClass()));
    }

    @Override
    @SuppressWarnings("deprecation")
    public <TResult> com.mongodb.client.MapReduceIterable<TResult> mapReduce(
            final ClientSession clientSession, final String mapFunction,
            final String reduceFunction, final Class<TResult> resultClass) {
        return new SyncMapReduceIterable<>(wrapped.mapReduce(unwrap(clientSession), mapFunction, reduceFunction, resultClass));
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests) {
        return requireNonNull(Mono.from(wrapped.bulkWrite(requests)).block(TIMEOUT_DURATION));
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options) {
        return requireNonNull(Mono.from(wrapped.bulkWrite(requests, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public BulkWriteResult bulkWrite(final ClientSession clientSession, final List<? extends WriteModel<? extends T>> requests) {
        return requireNonNull(Mono.from(wrapped.bulkWrite(unwrap(clientSession), requests)).block(TIMEOUT_DURATION));
    }

    @Override
    public BulkWriteResult bulkWrite(
            final ClientSession clientSession, final List<? extends WriteModel<? extends T>> requests,
            final BulkWriteOptions options) {
        return requireNonNull(Mono.from(wrapped.bulkWrite(unwrap(clientSession), requests, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public InsertOneResult insertOne(final T t) {
        return requireNonNull(Mono.from(wrapped.insertOne(t)).block(TIMEOUT_DURATION));
    }

    @Override
    public InsertOneResult insertOne(final T t, final InsertOneOptions options) {
        return requireNonNull(Mono.from(wrapped.insertOne(t, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public InsertOneResult insertOne(final ClientSession clientSession, final T t) {
        return requireNonNull(Mono.from(wrapped.insertOne(unwrap(clientSession), t)).block(TIMEOUT_DURATION));
    }

    @Override
    public InsertOneResult insertOne(final ClientSession clientSession, final T t, final InsertOneOptions options) {
        return requireNonNull(Mono.from(wrapped.insertOne(unwrap(clientSession), t, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public InsertManyResult insertMany(final List<? extends T> documents) {
        return requireNonNull(Mono.from(wrapped.insertMany(documents)).block(TIMEOUT_DURATION));
    }

    @Override
    public InsertManyResult insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        return requireNonNull(Mono.from(wrapped.insertMany(documents, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public InsertManyResult insertMany(final ClientSession clientSession, final List<? extends T> documents) {
        return requireNonNull(Mono.from(wrapped.insertMany(unwrap(clientSession), documents)).block(TIMEOUT_DURATION));
    }

    @Override
    public InsertManyResult insertMany(
            final ClientSession clientSession, final List<? extends T> documents,
            final InsertManyOptions options) {
        return requireNonNull(Mono.from(wrapped.insertMany(unwrap(clientSession), documents, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public DeleteResult deleteOne(final Bson filter) {
        return requireNonNull(Mono.from(wrapped.deleteOne(filter)).block(TIMEOUT_DURATION));
    }

    @Override
    public DeleteResult deleteOne(final Bson filter, final DeleteOptions options) {
        return requireNonNull(Mono.from(wrapped.deleteOne(filter, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public DeleteResult deleteOne(final ClientSession clientSession, final Bson filter) {
        return requireNonNull(Mono.from(wrapped.deleteOne(unwrap(clientSession), filter)).block(TIMEOUT_DURATION));
    }

    @Override
    public DeleteResult deleteOne(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        return requireNonNull(Mono.from(wrapped.deleteOne(unwrap(clientSession), filter, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public DeleteResult deleteMany(final Bson filter) {
        return requireNonNull(Mono.from(wrapped.deleteMany(filter)).block(TIMEOUT_DURATION));
    }

    @Override
    public DeleteResult deleteMany(final Bson filter, final DeleteOptions options) {
        return requireNonNull(Mono.from(wrapped.deleteMany(filter, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public DeleteResult deleteMany(final ClientSession clientSession, final Bson filter) {
        return requireNonNull(Mono.from(wrapped.deleteMany(unwrap(clientSession), filter)).block(TIMEOUT_DURATION));
    }

    @Override
    public DeleteResult deleteMany(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        return requireNonNull(Mono.from(wrapped.deleteMany(unwrap(clientSession), filter, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult replaceOne(final Bson filter, final T replacement) {
        return requireNonNull(Mono.from(wrapped.replaceOne(filter, replacement)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult replaceOne(final Bson filter, final T replacement, final ReplaceOptions replaceOptions) {
        return requireNonNull(Mono.from(wrapped.replaceOne(filter, replacement, replaceOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult replaceOne(final ClientSession clientSession, final Bson filter, final T replacement) {
        return requireNonNull(Mono.from(wrapped.replaceOne(unwrap(clientSession), filter, replacement)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult replaceOne(
            final ClientSession clientSession, final Bson filter, final T replacement,
            final ReplaceOptions replaceOptions) {
        return requireNonNull(Mono.from(wrapped.replaceOne(unwrap(clientSession), filter, replacement, replaceOptions))
                                      .block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final Bson update) {
        return requireNonNull(Mono.from(wrapped.updateOne(filter, update)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        return requireNonNull(Mono.from(wrapped.updateOne(filter, update, updateOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final Bson update) {
        return requireNonNull(Mono.from(wrapped.updateOne(unwrap(clientSession), filter, update)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateOne(
            final ClientSession clientSession, final Bson filter, final Bson update,
            final UpdateOptions updateOptions) {
        return requireNonNull(Mono.from(wrapped.updateOne(unwrap(clientSession), filter, update, updateOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final List<? extends Bson> update) {
        return requireNonNull(Mono.from(wrapped.updateOne(filter, update)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final List<? extends Bson> update, final UpdateOptions updateOptions) {
        return requireNonNull(Mono.from(wrapped.updateOne(filter, update, updateOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return requireNonNull(Mono.from(wrapped.updateOne(unwrap(clientSession), filter, update)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateOne(
            final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
            final UpdateOptions updateOptions) {
        return requireNonNull(Mono.from(wrapped.updateOne(unwrap(clientSession), filter, update, updateOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final Bson update) {
        return requireNonNull(Mono.from(wrapped.updateMany(filter, update)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        return requireNonNull(Mono.from(wrapped.updateMany(filter, update, updateOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final Bson update) {
        return requireNonNull(Mono.from(wrapped.updateMany(unwrap(clientSession), filter, update)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateMany(
            final ClientSession clientSession, final Bson filter, final Bson update,
            final UpdateOptions updateOptions) {
        return requireNonNull(Mono.from(wrapped.updateMany(unwrap(clientSession), filter, update, updateOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final List<? extends Bson> update) {
        return requireNonNull(Mono.from(wrapped.updateMany(filter, update)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final List<? extends Bson> update, final UpdateOptions updateOptions) {
        return requireNonNull(Mono.from(wrapped.updateMany(filter, update, updateOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return requireNonNull(Mono.from(wrapped.updateMany(unwrap(clientSession), filter, update)).block(TIMEOUT_DURATION));
    }

    @Override
    public UpdateResult updateMany(
            final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
            final UpdateOptions updateOptions) {
        return requireNonNull(Mono.from(wrapped.updateMany(unwrap(clientSession), filter, update, updateOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public T findOneAndDelete(final Bson filter) {
        return Mono.from(wrapped.findOneAndDelete(filter)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return Mono.from(wrapped.findOneAndDelete(filter, options)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndDelete(final ClientSession clientSession, final Bson filter) {
        return Mono.from(wrapped.findOneAndDelete(unwrap(clientSession), filter)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndDelete(final ClientSession clientSession, final Bson filter, final FindOneAndDeleteOptions options) {
        return Mono.from(wrapped.findOneAndDelete(unwrap(clientSession), filter, options)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndReplace(final Bson filter, final T replacement) {
        return Mono.from(wrapped.findOneAndReplace(filter, replacement)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndReplace(final Bson filter, final T replacement, final FindOneAndReplaceOptions options) {
        return Mono.from(wrapped.findOneAndReplace(filter, replacement, options)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndReplace(final ClientSession clientSession, final Bson filter, final T replacement) {
        return Mono.from(wrapped.findOneAndReplace(unwrap(clientSession), filter, replacement)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndReplace(
            final ClientSession clientSession, final Bson filter, final T replacement,
            final FindOneAndReplaceOptions options) {
        return Mono.from(wrapped.findOneAndReplace(unwrap(clientSession), filter, replacement, options))
                                      .block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final Bson update) {
        return Mono.from(wrapped.findOneAndUpdate(filter, update)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        return Mono.from(wrapped.findOneAndUpdate(filter, update, options)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update) {
        return Mono.from(wrapped.findOneAndUpdate(unwrap(clientSession), filter, update)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndUpdate(
            final ClientSession clientSession, final Bson filter, final Bson update,
            final FindOneAndUpdateOptions options) {
        return Mono.from(wrapped.findOneAndUpdate(unwrap(clientSession), filter, update, options)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final List<? extends Bson> update) {
        return Mono.from(wrapped.findOneAndUpdate(filter, update)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndUpdate(final Bson filter, final List<? extends Bson> update, final FindOneAndUpdateOptions options) {
        return Mono.from(wrapped.findOneAndUpdate(filter, update, options)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndUpdate(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return Mono.from(wrapped.findOneAndUpdate(unwrap(clientSession), filter, update)).block(TIMEOUT_DURATION);
    }

    @Override
    public T findOneAndUpdate(
            final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
            final FindOneAndUpdateOptions options) {
        return Mono.from(wrapped.findOneAndUpdate(unwrap(clientSession), filter, update)).block(TIMEOUT_DURATION);
    }

    @Override
    public void drop() {
        Mono.from(wrapped.drop()).block(TIMEOUT_DURATION);
    }

    @Override
    public void drop(final ClientSession clientSession) {
        Mono.from(wrapped.drop(unwrap(clientSession))).block(TIMEOUT_DURATION);
    }

    @Override
    public String createIndex(final Bson keys) {
        return requireNonNull(Mono.from(wrapped.createIndex(keys)).block(TIMEOUT_DURATION));
    }

    @Override
    public String createIndex(final Bson keys, final IndexOptions indexOptions) {
        return requireNonNull(Mono.from(wrapped.createIndex(keys, indexOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public String createIndex(final ClientSession clientSession, final Bson keys) {
        return requireNonNull(Mono.from(wrapped.createIndex(unwrap(clientSession), keys)).block(TIMEOUT_DURATION));
    }

    @Override
    public String createIndex(final ClientSession clientSession, final Bson keys, final IndexOptions indexOptions) {
        return requireNonNull(Mono.from(wrapped.createIndex(unwrap(clientSession), keys, indexOptions)).block(TIMEOUT_DURATION));
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
    public List<String> createIndexes(
            final ClientSession clientSession, final List<IndexModel> indexes,
            final CreateIndexOptions createIndexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIndexesIterable<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(final Class<TResult> resultClass) {
        return new SyncListIndexesIterable<>(wrapped.listIndexes(resultClass));
    }

    @Override
    public ListIndexesIterable<Document> listIndexes(final ClientSession clientSession) {
        return listIndexes(clientSession, Document.class);
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(final ClientSession clientSession, final Class<TResult> resultClass) {
        return new SyncListIndexesIterable<>(wrapped.listIndexes(unwrap(clientSession), resultClass));
    }

    @Override
    public void dropIndex(final String indexName) {
        Mono.from(wrapped.dropIndex(indexName)).block(TIMEOUT_DURATION);
    }

    @Override
    public void dropIndex(final String indexName, final DropIndexOptions dropIndexOptions) {
        Mono.from(wrapped.dropIndex(indexName, dropIndexOptions)).block(TIMEOUT_DURATION);
    }

    @Override
    public void dropIndex(final Bson keys) {
        Mono.from(wrapped.dropIndex(keys)).block(TIMEOUT_DURATION);
    }

    @Override
    public void dropIndex(final Bson keys, final DropIndexOptions dropIndexOptions) {
        Mono.from(wrapped.dropIndex(keys, dropIndexOptions)).block(TIMEOUT_DURATION);
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final String indexName) {
        Mono.from(wrapped.dropIndex(unwrap(clientSession), indexName)).block(TIMEOUT_DURATION);
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final Bson keys) {
        Mono.from(wrapped.dropIndex(unwrap(clientSession), keys)).block(TIMEOUT_DURATION);
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final String indexName, final DropIndexOptions dropIndexOptions) {
        Mono.from(wrapped.dropIndex(unwrap(clientSession), indexName, dropIndexOptions)).block(TIMEOUT_DURATION);
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final Bson keys, final DropIndexOptions dropIndexOptions) {
        Mono.from(wrapped.dropIndex(unwrap(clientSession), keys, dropIndexOptions)).block(TIMEOUT_DURATION);
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
    public void renameCollection(
            final ClientSession clientSession, final MongoNamespace newCollectionNamespace,
            final RenameCollectionOptions renameCollectionOptions) {
        throw new UnsupportedOperationException();
    }

    private com.mongodb.reactivestreams.client.ClientSession unwrap(final ClientSession clientSession) {
        return ((SyncClientSession) clientSession).getWrapped();
    }
}
