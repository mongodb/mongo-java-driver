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
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;


/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class GridFSUploadPublisherImpl implements GridFSUploadPublisher<Void> {

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
        Mono.<Void>create(sink -> {
            AtomicBoolean terminated = new AtomicBoolean(false);
            sink.onCancel(() -> createCancellationMono(terminated).subscribe());

            Consumer<Throwable> errorHandler = e -> createCancellationMono(terminated)
                    .doOnError(i -> sink.error(e))
                    .doOnSuccess(i -> sink.error(e))
                    .subscribe();

            Consumer<Long> saveFileDataMono = l -> createSaveFileDataMono(terminated, l)
                    .doOnError(errorHandler)
                    .doOnSuccess(i -> sink.success())
                    .subscribe();

            Consumer<Void> saveChunksMono = i -> createSaveChunksMono(terminated)
                    .doOnError(errorHandler)
                    .doOnSuccess(saveFileDataMono)
                    .subscribe();

            createCheckAndCreateIndexesMono()
                    .doOnError(errorHandler)
                    .doOnSuccess(saveChunksMono)
                    .subscribe();
        })
       .subscribe(s);
    }

    public GridFSUploadPublisher<ObjectId> withObjectId() {
        final GridFSUploadPublisherImpl wrapped = this;
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

    private Mono<Void> createCheckAndCreateIndexesMono() {
        MongoCollection<Document> collection = filesCollection.withDocumentClass(Document.class).withReadPreference(primary());
        FindPublisher<Document> findPublisher;
        if (clientSession != null) {
            findPublisher = collection.find(clientSession);
        } else {
            findPublisher = collection.find();
        }
        AtomicBoolean collectionExists = new AtomicBoolean(false);

        return Mono.create(sink -> Mono.from(findPublisher.projection(PROJECTION).first())
                .subscribe(
                        d -> collectionExists.set(true),
                        sink::error,
                        () -> {
                            if (collectionExists.get()) {
                                sink.success();
                            } else {
                                checkAndCreateIndex(filesCollection.withReadPreference(primary()), FILES_INDEX)
                                        .doOnError(sink::error)
                                        .doOnSuccess(i -> {
                                            checkAndCreateIndex(chunksCollection.withReadPreference(primary()), CHUNKS_INDEX)
                                                    .doOnError(sink::error)
                                                    .doOnSuccess(sink::success)
                                                    .subscribe();
                                        })
                                        .subscribe();
                            }
                        })
        );
    }

    private <T> Mono<Boolean> hasIndex(final MongoCollection<T> collection, final Document index) {
        ListIndexesPublisher<Document> listIndexesPublisher;
        if (clientSession != null) {
            listIndexesPublisher = collection.listIndexes(clientSession);
        } else {
            listIndexesPublisher = collection.listIndexes();
        }

        return Flux.from(listIndexesPublisher)
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

    private <T> Mono<Void> checkAndCreateIndex(final MongoCollection<T> collection, final Document index) {
        return hasIndex(collection, index).flatMap(hasIndex -> {
            if (!hasIndex) {
                return createIndexMono(collection, index).flatMap(s -> Mono.empty());
            } else {
                return Mono.empty();
            }
        });
    }

    private <T> Mono<String> createIndexMono(final MongoCollection<T> collection, final Document index) {
        return Mono.from(clientSession == null ? collection.createIndex(index) : collection.createIndex(clientSession, index));
    }

    private Mono<Long> createSaveChunksMono(final AtomicBoolean terminated) {
        return Mono.create(sink -> {
            final AtomicLong lengthInBytes = new AtomicLong(0);
            final AtomicInteger chunkIndex = new AtomicInteger(0);
            new ResizingByteBufferFlux(source, chunkSizeBytes)
                    .flatMap((Function<ByteBuffer, Publisher<InsertOneResult>>) byteBuffer -> {
                        if (terminated.get()) {
                            return Mono.empty();
                        }
                        byte[] byteArray = new byte[byteBuffer.remaining()];
                        if (byteBuffer.hasArray()) {
                            System.arraycopy(byteBuffer.array(), byteBuffer.position(), byteArray, 0, byteBuffer.remaining());
                        } else {
                            byteBuffer.mark();
                            byteBuffer.get(byteArray);
                            byteBuffer.reset();
                        }
                        Binary data = new Binary(byteArray);
                        lengthInBytes.addAndGet(data.length());

                        Document chunkDocument = new Document("files_id", fileId)
                                .append("n", chunkIndex.getAndIncrement())
                                .append("data", data);

                        return clientSession == null ? chunksCollection.insertOne(chunkDocument)
                                : chunksCollection.insertOne(clientSession, chunkDocument);
                    })
                    .subscribe(null, sink::error, () -> sink.success(lengthInBytes.get()));
        });
    }

    private Mono<InsertOneResult> createSaveFileDataMono(final AtomicBoolean terminated, final long lengthInBytes) {
        if (terminated.compareAndSet(false, true)) {
            GridFSFile gridFSFile = new GridFSFile(fileId, filename, lengthInBytes, chunkSizeBytes, new Date(), metadata);
            if (clientSession != null) {
                return Mono.from(filesCollection.insertOne(clientSession, gridFSFile));
            } else {
                return Mono.from(filesCollection.insertOne(gridFSFile));
            }
        } else {
            return Mono.empty();
        }
    }

    private Mono<DeleteResult> createCancellationMono(final AtomicBoolean terminated) {
        if (terminated.compareAndSet(false, true)) {
            if (clientSession != null) {
                return Mono.from(chunksCollection.deleteMany(clientSession, new Document("files_id", fileId)));
            } else {
                return Mono.from(chunksCollection.deleteMany(new Document("files_id", fileId)));
            }
        } else {
            return Mono.empty();
        }
    }

}
