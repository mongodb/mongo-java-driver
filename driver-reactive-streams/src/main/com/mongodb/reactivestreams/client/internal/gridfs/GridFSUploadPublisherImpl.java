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

package com.mongodb.reactivestreams.client.internal.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.reactivestreams.client.internal.TimeoutHelper.collectionWithTimeout;
import static com.mongodb.reactivestreams.client.internal.TimeoutHelper.collectionWithTimeoutDeferred;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class GridFSUploadPublisherImpl implements GridFSUploadPublisher<Void> {

    private static final String TIMEOUT_ERROR_MESSAGE_CHUNKS_SAVING = "Saving chunks exceeded the timeout limit.";
    private static final String TIMEOUT_ERROR_MESSAGE_UPLOAD_CANCELLATION = "Upload cancellation exceeded the timeout limit.";
    private static final Document PROJECTION = new Document("_id", 1);
    private static final Document FILES_INDEX = new Document("filename", 1).append("uploadDate", 1);
    private static final Document CHUNKS_INDEX = new Document("files_id", 1).append("n", 1);
    private final ClientSession clientSession;
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<Document> chunksCollection;
    private final BsonValue fileId;
    private final String filename;
    private final int chunkSizeBytes;
    private final Document metadata;
    private final Publisher<ByteBuffer> source;
    @Nullable
    private final Long timeoutMs;

    public GridFSUploadPublisherImpl(@Nullable final ClientSession clientSession,
                                     final MongoCollection<GridFSFile> filesCollection,
                                     final MongoCollection<Document> chunksCollection,
                                     final BsonValue fileId,
                                     final String filename,
                                     final int chunkSizeBytes,
                                     @Nullable final Document metadata,
                                     final Publisher<ByteBuffer> source) {
        this.clientSession = clientSession;
        this.filesCollection = notNull("files collection", filesCollection);
        this.chunksCollection = notNull("chunks collection", chunksCollection);
        this.fileId = notNull("File Id", fileId);
        this.filename = notNull("filename", filename);
        this.chunkSizeBytes = chunkSizeBytes;
        this.metadata = metadata;
        this.source = source;
        this.timeoutMs = filesCollection.getTimeout(MILLISECONDS);
    }

    @Override
    public ObjectId getObjectId() {
        if (!fileId.isObjectId()) {
            throw new MongoGridFSException("Custom id type used for this GridFS upload stream");
        }
        return fileId.asObjectId().getValue();
    }

    @Override
    public BsonValue getId() {
        return fileId;
    }

    @Override
    public void subscribe(final Subscriber<? super Void> s) {
        Mono.deferContextual(ctx -> {
            AtomicBoolean terminated = new AtomicBoolean(false);
            Timeout timeout = TimeoutContext.startTimeout(timeoutMs);
            return createCheckAndCreateIndexesMono(timeout)
                    .then(createSaveChunksMono(terminated, timeout))
                    .flatMap(lengthInBytes -> createSaveFileDataMono(terminated, lengthInBytes, timeout))
                    .onErrorResume(originalError ->
                            createCancellationMono(terminated, timeout)
                                    .onErrorMap(cancellationError -> {
                                        // Timeout exception might occur during cancellation. It gets suppressed.
                                        originalError.addSuppressed(cancellationError);
                                        return originalError;
                                    })
                                    .then(Mono.error(originalError)))
                    .doOnCancel(() -> createCancellationMono(terminated, timeout).contextWrite(ctx).subscribe())
                    .then();
        }).subscribe(s);
    }

    public GridFSUploadPublisher<ObjectId> withObjectId() {
        GridFSUploadPublisherImpl wrapped = this;
        return new GridFSUploadPublisher<ObjectId>() {

            @Override
            public ObjectId getObjectId() {
                return wrapped.getObjectId();
            }

            @Override
            public BsonValue getId() {
                return wrapped.getId();
            }

            @Override
            public void subscribe(final Subscriber<? super ObjectId> subscriber) {
                Mono.from(wrapped)
                        .thenReturn(getObjectId())
                .subscribe(subscriber);
            }
        };
    }

    private Mono<Void> createCheckAndCreateIndexesMono(@Nullable final Timeout timeout) {
        return collectionWithTimeoutDeferred(filesCollection.withDocumentClass(Document.class).withReadPreference(primary()), timeout)
                .map(collection -> clientSession != null ? collection.find(clientSession) : collection.find())
                .flatMap(findPublisher -> Mono.from(findPublisher.projection(PROJECTION).first()))
                .switchIfEmpty(Mono.defer(() ->
                        checkAndCreateIndex(filesCollection.withReadPreference(primary()), FILES_INDEX, timeout)
                                .then(checkAndCreateIndex(chunksCollection.withReadPreference(primary()), CHUNKS_INDEX, timeout))
                                .then(Mono.empty())
                ))
                .then();
    }

    private <T> Mono<Boolean> hasIndex(final MongoCollection<T> collection, final Document index, @Nullable final Timeout timeout) {
        return collectionWithTimeoutDeferred(collection, timeout)
                .map(wrappedCollection -> {
                    if (clientSession != null) {
                        return wrappedCollection.listIndexes(clientSession);
                    } else {
                        return wrappedCollection.listIndexes();
                    }
                }).flatMapMany(Flux::from)
                .collectList()
                .map(indexes -> {
                    boolean hasIndex = false;
                    for (Document result : indexes) {
                        Document indexDoc = result.get("key", new Document());
                        for (final Map.Entry<String, Object> entry : indexDoc.entrySet()) {
                            if (entry.getValue() instanceof Number) {
                                entry.setValue(((Number) entry.getValue()).intValue());
                            }
                        }
                        if (indexDoc.equals(index)) {
                            hasIndex = true;
                            break;
                        }
                    }
                    return hasIndex;
                });
    }

    private <T> Mono<Void> checkAndCreateIndex(final MongoCollection<T> collection, final Document index, @Nullable final Timeout timeout) {
        return hasIndex(collection, index, timeout).flatMap(hasIndex -> {
            if (!hasIndex) {
                return createIndexMono(collection, index, timeout).flatMap(s -> Mono.empty());
            } else {
                return Mono.empty();
            }
        });
    }

    private <T> Mono<String> createIndexMono(final MongoCollection<T> collection, final Document index, @Nullable final Timeout timeout) {
        return collectionWithTimeoutDeferred(collection, timeout).flatMap(wrappedCollection ->
             Mono.from(clientSession == null ? wrappedCollection.createIndex(index) : wrappedCollection.createIndex(clientSession, index))
        );
    }

    private Mono<Long> createSaveChunksMono(final AtomicBoolean terminated, @Nullable final Timeout timeout) {
        return new ResizingByteBufferFlux(source, chunkSizeBytes)
                .takeUntilOther(createMonoTimer(timeout))
                .index()
                .flatMap(indexAndBuffer -> {
                    if (terminated.get()) {
                        return Mono.empty();
                    }
                    Long index = indexAndBuffer.getT1();
                    ByteBuffer byteBuffer = indexAndBuffer.getT2();
                    byte[] byteArray = new byte[byteBuffer.remaining()];
                    if (byteBuffer.hasArray()) {
                        System.arraycopy(byteBuffer.array(), byteBuffer.position(), byteArray, 0, byteBuffer.remaining());
                    } else {
                        byteBuffer.mark();
                        byteBuffer.get(byteArray);
                        byteBuffer.reset();
                    }
                    Binary data = new Binary(byteArray);

                    Document chunkDocument = new Document("files_id", fileId)
                            .append("n", index.intValue())
                            .append("data", data);

                    Publisher<InsertOneResult> insertOnePublisher = clientSession == null
                            ? collectionWithTimeout(chunksCollection, timeout, TIMEOUT_ERROR_MESSAGE_CHUNKS_SAVING).insertOne(chunkDocument)
                            : collectionWithTimeout(chunksCollection, timeout, TIMEOUT_ERROR_MESSAGE_CHUNKS_SAVING)
                               .insertOne(clientSession, chunkDocument);

                    return Mono.from(insertOnePublisher).thenReturn(data.length());
                })
                .reduce(0L, Long::sum);
    }

    /**
     * Creates a Mono that emits a {@link MongoOperationTimeoutException} after the specified timeout.
     *
     * @param timeout - remaining timeout.
     * @return Mono that emits a {@link MongoOperationTimeoutException}.
     */
    private static Mono<MongoOperationTimeoutException> createMonoTimer(final @Nullable Timeout timeout) {
        return Timeout.nullAsInfinite(timeout).call(MILLISECONDS,
                () -> Mono.never(),
                (ms) -> Mono.delay(ofMillis(ms)).then(createTimeoutMonoError()),
                () -> createTimeoutMonoError());
    }

    private static Mono<MongoOperationTimeoutException> createTimeoutMonoError() {
        return Mono.error(TimeoutContext.createMongoTimeoutException(
                "GridFS waiting for data from the source Publisher exceeded the timeout limit."));
    }

    private Mono<InsertOneResult> createSaveFileDataMono(final AtomicBoolean terminated,
                                                         final long lengthInBytes,
                                                         @Nullable final Timeout timeout) {
        Mono<MongoCollection<GridFSFile>> filesCollectionMono = collectionWithTimeoutDeferred(filesCollection, timeout);
        if (terminated.compareAndSet(false, true)) {
            GridFSFile gridFSFile = new GridFSFile(fileId, filename, lengthInBytes, chunkSizeBytes, new Date(), metadata);
            if (clientSession != null) {
                return filesCollectionMono.flatMap(collection -> Mono.from(collection.insertOne(clientSession, gridFSFile)));
            } else {
                return filesCollectionMono.flatMap(collection -> Mono.from(collection.insertOne(gridFSFile)));
            }
        } else {
            return Mono.empty();
        }
    }

    private Mono<DeleteResult> createCancellationMono(final AtomicBoolean terminated, @Nullable final Timeout timeout) {
        Mono<MongoCollection<Document>> chunksCollectionMono = collectionWithTimeoutDeferred(chunksCollection, timeout,
                TIMEOUT_ERROR_MESSAGE_UPLOAD_CANCELLATION);
        if (terminated.compareAndSet(false, true)) {
            if (clientSession != null) {
                return chunksCollectionMono.flatMap(collection -> Mono.from(collection
                        .deleteMany(clientSession, new Document("files_id", fileId))));
            } else {
                return chunksCollectionMono.flatMap(collection -> Mono.from(collection
                        .deleteMany(new Document("files_id", fileId))));
            }
        } else {
            return Mono.empty();
        }
    }
}
