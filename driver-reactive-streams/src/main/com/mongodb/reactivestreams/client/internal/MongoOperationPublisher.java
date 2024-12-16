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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.CreateViewOptions;
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
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.operation.AsyncOperations;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.internal.operation.IndexHelper;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class MongoOperationPublisher<T> {

    private final AsyncOperations<T> operations;
    private final UuidRepresentation uuidRepresentation;
    @Nullable
    private final AutoEncryptionSettings autoEncryptionSettings;
    private final OperationExecutor executor;

    MongoOperationPublisher(
            final Class<T> documentClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
            final ReadConcern readConcern, final WriteConcern writeConcern, final boolean retryWrites, final boolean retryReads,
            final UuidRepresentation uuidRepresentation, @Nullable final AutoEncryptionSettings autoEncryptionSettings,
            final TimeoutSettings timeoutSettings, final OperationExecutor executor) {
        this(new MongoNamespace("_ignored", "_ignored"), documentClass,
             codecRegistry, readPreference, readConcern, writeConcern, retryWrites, retryReads,
             uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
    }

    MongoOperationPublisher(
            final MongoNamespace namespace, final Class<T> documentClass, final CodecRegistry codecRegistry,
            final ReadPreference readPreference, final ReadConcern readConcern, final WriteConcern writeConcern,
            final boolean retryWrites, final boolean retryReads, final UuidRepresentation uuidRepresentation,
            @Nullable final AutoEncryptionSettings autoEncryptionSettings, final TimeoutSettings timeoutSettings,
            final OperationExecutor executor) {
        this.operations = new AsyncOperations<>(namespace, notNull("documentClass", documentClass),
                                           notNull("readPreference", readPreference), notNull("codecRegistry", codecRegistry),
                                           notNull("readConcern", readConcern), notNull("writeConcern", writeConcern),
                                           retryWrites, retryReads, timeoutSettings);
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
        this.autoEncryptionSettings = autoEncryptionSettings;
        this.executor = notNull("executor", executor);
    }

    MongoNamespace getNamespace() {
        return operations.getNamespace();
    }

    ReadPreference getReadPreference() {
        return operations.getReadPreference();
    }

    CodecRegistry getCodecRegistry() {
        return operations.getCodecRegistry();
    }

    ReadConcern getReadConcern() {
        return operations.getReadConcern();
    }

    WriteConcern getWriteConcern() {
        return operations.getWriteConcern();
    }

    public boolean getRetryWrites() {
        return operations.isRetryWrites();
    }

    public boolean getRetryReads() {
        return operations.isRetryReads();
    }

    @Nullable
    public Long getTimeoutMS() {
        return getTimeoutSettings().getTimeoutMS();
    }

    public TimeoutSettings getTimeoutSettings() {
        return operations.getTimeoutSettings();
    }

    Class<T> getDocumentClass() {
        return operations.getDocumentClass();
    }

    public AsyncOperations<T> getOperations() {
        return operations;
    }

    MongoOperationPublisher<T> withDatabase(final String name) {
        return withDatabaseAndDocumentClass(name, getDocumentClass());
    }

    <D> MongoOperationPublisher<D> withDatabaseAndDocumentClass(final String name, final Class<D> documentClass) {
        return withNamespaceAndDocumentClass(new MongoNamespace(notNull("name", name), "ignored"),
                                             notNull("documentClass", documentClass));
    }

    MongoOperationPublisher<T> withNamespace(final MongoNamespace namespace) {
        return withNamespaceAndDocumentClass(namespace, getDocumentClass());
    }

    <D> MongoOperationPublisher<D> withDocumentClass(final Class<D> documentClass) {
        return withNamespaceAndDocumentClass(getNamespace(), documentClass);
    }

    @SuppressWarnings("unchecked")
    <D> MongoOperationPublisher<D> withNamespaceAndDocumentClass(final MongoNamespace namespace, final Class<D> documentClass) {
        if (getNamespace().equals(namespace) && getDocumentClass().equals(documentClass)) {
            return (MongoOperationPublisher<D>) this;
        }
        return new MongoOperationPublisher<>(notNull("namespace", namespace), notNull("documentClass", documentClass),
                getCodecRegistry(), getReadPreference(), getReadConcern(), getWriteConcern(), getRetryWrites(), getRetryReads(),
                uuidRepresentation, autoEncryptionSettings, getTimeoutSettings(), executor);
    }

    MongoOperationPublisher<T> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoOperationPublisher<>(getNamespace(), getDocumentClass(),
                withUuidRepresentation(notNull("codecRegistry", codecRegistry), uuidRepresentation),
                getReadPreference(), getReadConcern(), getWriteConcern(), getRetryWrites(), getRetryReads(),
                uuidRepresentation, autoEncryptionSettings, getTimeoutSettings(), executor);
    }

    MongoOperationPublisher<T> withReadPreference(final ReadPreference readPreference) {
        if (getReadPreference().equals(readPreference)) {
            return this;
        }
        return new MongoOperationPublisher<>(getNamespace(), getDocumentClass(), getCodecRegistry(),
                notNull("readPreference", readPreference), getReadConcern(), getWriteConcern(), getRetryWrites(), getRetryReads(),
                uuidRepresentation, autoEncryptionSettings, getTimeoutSettings(), executor);
    }

    MongoOperationPublisher<T> withWriteConcern(final WriteConcern writeConcern) {
        if (getWriteConcern().equals(writeConcern)) {
            return this;
        }
        return new MongoOperationPublisher<>(getNamespace(), getDocumentClass(), getCodecRegistry(), getReadPreference(), getReadConcern(),
                notNull("writeConcern", writeConcern), getRetryWrites(), getRetryReads(), uuidRepresentation, autoEncryptionSettings,
                getTimeoutSettings(), executor);
    }

    MongoOperationPublisher<T> withReadConcern(final ReadConcern readConcern) {
        if (getReadConcern().equals(readConcern)) {
            return this;
        }
        return new MongoOperationPublisher<>(getNamespace(), getDocumentClass(),
                getCodecRegistry(), getReadPreference(), notNull("readConcern", readConcern),
                getWriteConcern(), getRetryWrites(), getRetryReads(), uuidRepresentation,
                autoEncryptionSettings, getTimeoutSettings(), executor);
    }

    MongoOperationPublisher<T> withTimeout(final long timeout, final TimeUnit timeUnit) {
        TimeoutSettings timeoutSettings = getTimeoutSettings().withTimeout(timeout, timeUnit);
        if (Objects.equals(getTimeoutSettings(), timeoutSettings)) {
            return this;
        }
        return new MongoOperationPublisher<>(getNamespace(), getDocumentClass(),
                getCodecRegistry(), getReadPreference(), getReadConcern(),
                getWriteConcern(), getRetryWrites(), getRetryReads(), uuidRepresentation,
                autoEncryptionSettings, timeoutSettings, executor);
    }

    Publisher<Void> dropDatabase(@Nullable final ClientSession clientSession) {
        return createWriteOperationMono(operations::getTimeoutSettings, operations::dropDatabase, clientSession);
    }

    Publisher<Void> createCollection(
            @Nullable final ClientSession clientSession, final String collectionName, final CreateCollectionOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.createCollection(collectionName, options, autoEncryptionSettings), clientSession);
    }

    Publisher<Void> createView(
            @Nullable final ClientSession clientSession, final String viewName, final String viewOn,
            final List<? extends Bson> pipeline, final CreateViewOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.createView(viewName, viewOn, pipeline, options), clientSession);
    }

    public <R> Publisher<R> runCommand(
            @Nullable final ClientSession clientSession, final Bson command,
            final ReadPreference readPreference, final Class<R> clazz) {
        if (clientSession != null && clientSession.hasActiveTransaction() && !readPreference.equals(ReadPreference.primary())) {
            return Mono.error(new MongoClientException("Read preference in a transaction must be primary"));
        }
        return createReadOperationMono(
                operations::getTimeoutSettings,
                () -> operations.commandRead(command, clazz), clientSession, notNull("readPreference", readPreference));
    }


    Publisher<Long> estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        return createReadOperationMono(
                (asyncOperations -> asyncOperations.createTimeoutSettings(options)),
                () -> operations.estimatedDocumentCount(notNull("options", options)), null);
    }

    Publisher<Long> countDocuments(@Nullable final ClientSession clientSession, final Bson filter, final CountOptions options) {
        return createReadOperationMono(
                (asyncOperations -> asyncOperations.createTimeoutSettings(options)),
                () -> operations.countDocuments(notNull("filter", filter), notNull("options", options)
        ), clientSession);
    }

    Publisher<BulkWriteResult> bulkWrite(
            @Nullable final ClientSession clientSession,
            final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.bulkWrite(notNull("requests", requests), notNull("options", options)), clientSession);
    }

    Publisher<ClientBulkWriteResult> clientBulkWrite(
            @Nullable final ClientSession clientSession,
            final List<? extends ClientNamespacedWriteModel> clientWriteModels,
            @Nullable final ClientBulkWriteOptions options) {
        isTrue("`autoEncryptionSettings` is null, as bulkWrite does not currently support automatic encryption", autoEncryptionSettings == null);
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.clientBulkWriteOperation(clientWriteModels, options), clientSession);
    }

    Publisher<InsertOneResult> insertOne(@Nullable final ClientSession clientSession, final T document, final InsertOneOptions options) {
        return createSingleWriteRequestMono(() -> operations.insertOne(notNull("document", document),
                                                                       notNull("options", options)),
                                            clientSession, WriteRequest.Type.INSERT)
                .map(INSERT_ONE_RESULT_MAPPER);
    }

    Publisher<InsertManyResult> insertMany(
            @Nullable final ClientSession clientSession, final List<? extends T> documents,
            final InsertManyOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.insertMany(notNull("documents", documents), notNull("options", options)), clientSession)
                .map(INSERT_MANY_RESULT_MAPPER);
    }

    Publisher<DeleteResult> deleteOne(@Nullable final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        return createSingleWriteRequestMono(() -> operations.deleteOne(notNull("filter", filter), notNull("options", options)),
                                            clientSession, WriteRequest.Type.DELETE)
                .map(DELETE_RESULT_MAPPER);
    }

    Publisher<DeleteResult> deleteMany(@Nullable final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        return createSingleWriteRequestMono(() -> operations.deleteMany(notNull("filter", filter), notNull("options", options)),
                                            clientSession, WriteRequest.Type.DELETE)
                .map(DELETE_RESULT_MAPPER);
    }

    Publisher<UpdateResult> replaceOne(
            @Nullable final ClientSession clientSession, final Bson filter, final T replacement,
            final ReplaceOptions options) {
        return createSingleWriteRequestMono(() -> operations.replaceOne(notNull("filter", filter),
                                                                        notNull("replacement", replacement),
                                                                        notNull("options", options)),
                                            clientSession, WriteRequest.Type.REPLACE)
                .map(UPDATE_RESULT_MAPPER);
    }

    Publisher<UpdateResult> updateOne(
            @Nullable final ClientSession clientSession, final Bson filter, final Bson update,
            final UpdateOptions options) {
        return createSingleWriteRequestMono(() -> operations.updateOne(notNull("filter", filter),
                                                                       notNull("update", update),
                                                                       notNull("options", options)),
                                            clientSession, WriteRequest.Type.UPDATE)
                .map(UPDATE_RESULT_MAPPER);
    }

    Publisher<UpdateResult> updateOne(
            @Nullable final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
            final UpdateOptions options) {
        return createSingleWriteRequestMono(() -> operations.updateOne(notNull("filter", filter),
                                                                       notNull("update", update),
                                                                       notNull("options", options)),
                                            clientSession, WriteRequest.Type.UPDATE)
                .map(UPDATE_RESULT_MAPPER);
    }

    Publisher<UpdateResult> updateMany(
            @Nullable final ClientSession clientSession, final Bson filter, final Bson update,
            final UpdateOptions options) {
        return createSingleWriteRequestMono(() -> operations.updateMany(notNull("filter", filter),
                                                                        notNull("update", update),
                                                                        notNull("options", options)),
                                            clientSession, WriteRequest.Type.UPDATE)
                .map(UPDATE_RESULT_MAPPER);
    }

    Publisher<UpdateResult> updateMany(
            @Nullable final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
            final UpdateOptions options) {
        return createSingleWriteRequestMono(() -> operations.updateMany(notNull("filter", filter),
                                                                        notNull("update", update),
                                                                        notNull("options", options)),
                                            clientSession, WriteRequest.Type.UPDATE)
                .map(UPDATE_RESULT_MAPPER);
    }

    Publisher<T> findOneAndDelete(@Nullable final ClientSession clientSession, final Bson filter, final FindOneAndDeleteOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.findOneAndDelete(notNull("filter", filter), notNull("options", options)), clientSession);
    }

    Publisher<T> findOneAndReplace(
            @Nullable final ClientSession clientSession, final Bson filter, final T replacement,
            final FindOneAndReplaceOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.findOneAndReplace(notNull("filter", filter),
                                                                           notNull("replacement", replacement),
                                                                           notNull("options", options)),
                                        clientSession);
    }

    Publisher<T> findOneAndUpdate(
            @Nullable final ClientSession clientSession, final Bson filter, final Bson update,
            final FindOneAndUpdateOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.findOneAndUpdate(notNull("filter", filter),
                                                                          notNull("update", update),
                                                                          notNull("options", options)),
                                        clientSession);
    }

    Publisher<T> findOneAndUpdate(
            @Nullable final ClientSession clientSession, final Bson filter,
            final List<? extends Bson> update, final FindOneAndUpdateOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.findOneAndUpdate(notNull("filter", filter),
                                                                          notNull("update", update),
                                                                          notNull("options", options)),
                                        clientSession);
    }

    Publisher<Void> dropCollection(@Nullable final ClientSession clientSession, final DropCollectionOptions dropCollectionOptions) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.dropCollection(dropCollectionOptions, autoEncryptionSettings), clientSession);
    }

    Publisher<String> createIndex(@Nullable final ClientSession clientSession, final Bson key, final IndexOptions options) {
        return createIndexes(clientSession, singletonList(new IndexModel(notNull("key", key), options)), new CreateIndexOptions());
    }


    Publisher<String> createIndexes(
            @Nullable final ClientSession clientSession, final List<IndexModel> indexes,
            final CreateIndexOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.createIndexes(notNull("indexes", indexes), notNull("options", options)), clientSession)
                .thenMany(Flux.fromIterable(IndexHelper.getIndexNames(indexes, getCodecRegistry())));
    }

    Publisher<String> createSearchIndex(@Nullable final String indexName, final Bson definition) {
        SearchIndexModel searchIndexModel =
                indexName == null ? new SearchIndexModel(definition) : new SearchIndexModel(indexName, definition);

        return createSearchIndexes(singletonList(searchIndexModel));
    }

    Publisher<String> createSearchIndexes(final List<SearchIndexModel> indexes) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.createSearchIndexes(indexes), null)
                .thenMany(Flux.fromIterable(IndexHelper.getSearchIndexNames(indexes)));
    }


    public Publisher<Void> updateSearchIndex(final String name, final Bson definition) {
       return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.updateSearchIndex(name, definition), null);
    }


    public Publisher<Void> dropSearchIndex(final String indexName) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.dropSearchIndex(indexName), null);
    }

    Publisher<Void> dropIndex(@Nullable final ClientSession clientSession, final String indexName, final DropIndexOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.dropIndex(notNull("indexName", indexName), notNull("options", options)),
                                        clientSession);
    }

    Publisher<Void> dropIndex(@Nullable final ClientSession clientSession, final Bson keys, final DropIndexOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.dropIndex(notNull("keys", keys), notNull("options", options)),
                                        clientSession);
    }

    Publisher<Void> dropIndexes(@Nullable final ClientSession clientSession, final DropIndexOptions options) {
        return dropIndex(clientSession, "*", options);
    }

    Publisher<Void> renameCollection(
            @Nullable final ClientSession clientSession, final MongoNamespace newCollectionNamespace,
            final RenameCollectionOptions options) {
        return createWriteOperationMono(
                operations::getTimeoutSettings,
                () -> operations.renameCollection(notNull("newCollectionNamespace", newCollectionNamespace),
                                                                          notNull("options", options)),
                                        clientSession);
    }


    <R> Mono<R> createReadOperationMono(final Function<AsyncOperations<?>, TimeoutSettings> timeoutSettingsFunction,
            final Supplier<AsyncReadOperation<R>> operation, @Nullable final ClientSession clientSession) {
        return createReadOperationMono(() -> timeoutSettingsFunction.apply(operations), operation, clientSession, getReadPreference());
    }


    <R> Mono<R> createReadOperationMono(final Supplier<TimeoutSettings> timeoutSettingsSupplier,
            final Supplier<AsyncReadOperation<R>> operationSupplier, @Nullable final ClientSession clientSession,
            final ReadPreference readPreference) {
        AsyncReadOperation<R> readOperation = operationSupplier.get();
        return getExecutor(timeoutSettingsSupplier.get())
                .execute(readOperation, readPreference, getReadConcern(), clientSession);
    }

    <R> Mono<R> createWriteOperationMono(final Function<AsyncOperations<?>, TimeoutSettings> timeoutSettingsFunction,
            final Supplier<AsyncWriteOperation<R>> operationSupplier, @Nullable final ClientSession clientSession) {
        return createWriteOperationMono(() -> timeoutSettingsFunction.apply(operations), operationSupplier, clientSession);
    }

    <R> Mono<R> createWriteOperationMono(final Supplier<TimeoutSettings> timeoutSettingsSupplier,
            final Supplier<AsyncWriteOperation<R>> operationSupplier, @Nullable final ClientSession clientSession) {
        AsyncWriteOperation<R> writeOperation = operationSupplier.get();
        return  getExecutor(timeoutSettingsSupplier.get())
                .execute(writeOperation, getReadConcern(), clientSession);
    }

    private Mono<BulkWriteResult> createSingleWriteRequestMono(
            final Supplier<AsyncWriteOperation<BulkWriteResult>> operation,
            @Nullable final ClientSession clientSession,
            final WriteRequest.Type type) {
        return createWriteOperationMono(operations::getTimeoutSettings, operation, clientSession)
                .onErrorMap(MongoBulkWriteException.class, e -> {
                    MongoException exception;
                    WriteConcernError writeConcernError = e.getWriteConcernError();
                    if (e.getWriteErrors().isEmpty() && writeConcernError != null) {
                        WriteConcernResult writeConcernResult;
                        if (type == WriteRequest.Type.INSERT) {
                            writeConcernResult = WriteConcernResult.acknowledged(e.getWriteResult().getInsertedCount(), false, null);
                        } else if (type == WriteRequest.Type.DELETE) {
                            writeConcernResult = WriteConcernResult.acknowledged(e.getWriteResult().getDeletedCount(), false, null);
                        } else {
                            writeConcernResult = WriteConcernResult
                                    .acknowledged(e.getWriteResult().getMatchedCount() + e.getWriteResult().getUpserts().size(),
                                                  e.getWriteResult().getMatchedCount() > 0,
                                                  e.getWriteResult().getUpserts().isEmpty()
                                                          ? null : e.getWriteResult().getUpserts().get(0).getId());
                        }
                        exception = new MongoWriteConcernException(writeConcernError, writeConcernResult, e.getServerAddress(),
                                e.getErrorLabels());
                    } else if (!e.getWriteErrors().isEmpty()) {
                        exception = new MongoWriteException(new WriteError(e.getWriteErrors().get(0)), e.getServerAddress(),
                                e.getErrorLabels());
                    } else {
                        exception = new MongoWriteException(new WriteError(-1, "Unknown write error", new BsonDocument()),
                                                            e.getServerAddress(), e.getErrorLabels());
                    }

                    return exception;
                });
    }

    private OperationExecutor getExecutor(final TimeoutSettings timeoutSettings) {
        return executor.withTimeoutSettings(timeoutSettings);
    }

    private static final Function<BulkWriteResult, InsertOneResult> INSERT_ONE_RESULT_MAPPER = result -> {
        if (result.wasAcknowledged()) {
            BsonValue insertedId = result.getInserts().isEmpty() ? null : result.getInserts().get(0).getId();
            return InsertOneResult.acknowledged(insertedId);
        } else {
            return InsertOneResult.unacknowledged();
        }
    };
    private static final Function<BulkWriteResult, InsertManyResult> INSERT_MANY_RESULT_MAPPER = result -> {
        if (result.wasAcknowledged()) {
            return InsertManyResult.acknowledged(result.getInserts().stream()
                                                         .collect(HashMap::new, (m, v) -> m.put(v.getIndex(), v.getId()), HashMap::putAll));
        } else {
            return InsertManyResult.unacknowledged();
        }
    };
    private static final Function<BulkWriteResult, DeleteResult> DELETE_RESULT_MAPPER = result -> {
        if (result.wasAcknowledged()) {
            return DeleteResult.acknowledged(result.getDeletedCount());
        } else {
            return DeleteResult.unacknowledged();
        }
    };
    private static final Function<BulkWriteResult, UpdateResult> UPDATE_RESULT_MAPPER = result -> {
        if (result.wasAcknowledged()) {
            BsonValue upsertedId = result.getUpserts().isEmpty() ? null : result.getUpserts().get(0).getId();
            return UpdateResult.acknowledged(result.getMatchedCount(), (long) result.getModifiedCount(), upsertedId);
        } else {
            return UpdateResult.unacknowledged();
        }
    };

    public static <T> SingleResultCallback<T> sinkToCallback(final MonoSink<T> sink) {
        return (result, t) -> {
            if (t != null) {
                sink.error(t);
            } else if (result == null) {
                sink.success();
            } else {
                sink.success(result);
            }
        };
    }
}
