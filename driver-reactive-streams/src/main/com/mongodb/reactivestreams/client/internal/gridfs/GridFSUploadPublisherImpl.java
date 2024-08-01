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
import reactor.util.function.Tuple2;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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
        Mono.deferContextual(ctx -> {
            AtomicBoolean terminated = new AtomicBoolean(false);
            return createCheckAndCreateIndexesMono()
                    .then(createSaveChunksMono(terminated))
                    .flatMap(lengthInBytes -> createSaveFileDataMono(terminated, lengthInBytes))
                    .onErrorResume(originalError ->
                            createCancellationMono(terminated)
                                    .onErrorMap(cancellationError -> {
                                        // Timeout exception might occur during cancellation. It gets suppressed.
                                        originalError.addSuppressed(cancellationError);
                                        return originalError;
                                    })
                                    .then(Mono.error(originalError)))
                    .doOnCancel(() -> createCancellationMono(terminated).contextWrite(ctx).subscribe())
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

    private Mono<Void> createCheckAndCreateIndexesMono() {
        MongoCollection<Document> collection = filesCollection.withDocumentClass(Document.class).withReadPreference(primary());
        FindPublisher<Document> findPublisher;
        if (clientSession != null) {
            findPublisher = collection.find(clientSession);
        } else {
            findPublisher = collection.find();
        }
        return Mono.from(findPublisher.projection(PROJECTION).first())
                .switchIfEmpty(Mono.defer(() ->
                        checkAndCreateIndex(filesCollection.withReadPreference(primary()), FILES_INDEX)
                                .then(checkAndCreateIndex(chunksCollection.withReadPreference(primary()), CHUNKS_INDEX))
                                .then(Mono.fromCallable(Document::new))
                ))
                .then();

    }

    private <T> Mono<Boolean> hasIndex(final MongoCollection<T> collection, final Document index) {
        ListIndexesPublisher<Document> listIndexesPublisher;
        if (clientSession != null) {
            listIndexesPublisher = collection.listIndexes(clientSession);
        } else {
            listIndexesPublisher = collection.listIndexes();
        }

        return Flux.from(listIndexesPublisher)
                .filter((result) -> {
                    Document indexDoc = result.get("key", new Document());
                    for (final Map.Entry<String, Object> entry : indexDoc.entrySet()) {
                        if (entry.getValue() instanceof Number) {
                            entry.setValue(((Number) entry.getValue()).intValue());
                        }
                    }
                    return indexDoc.equals(index);
                })
                .take(1)
                .hasElements();
    }

    private <T> Mono<Void> checkAndCreateIndex(final MongoCollection<T> collection, final Document index) {
        return hasIndex(collection, index).flatMap(hasIndex -> {
            if (!hasIndex) {
                return createIndexMono(collection, index).then();
            } else {
                return Mono.empty();
            }
        });
    }

    private <T> Mono<String> createIndexMono(final MongoCollection<T> collection, final Document index) {
        return Mono.from(clientSession == null ? collection.createIndex(index) : collection.createIndex(clientSession, index));
    }

    private Mono<Long> createSaveChunksMono(final AtomicBoolean terminated) {
        return new ResizingByteBufferFlux(source, chunkSizeBytes)
                    .index()
                    .flatMap((Function<Tuple2<Long, ByteBuffer>, Publisher<Integer>>) indexAndBuffer -> {
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
                                ? chunksCollection.insertOne(chunkDocument)
                                : chunksCollection.insertOne(clientSession, chunkDocument);

                        return Mono.from(insertOnePublisher).thenReturn(data.length());
                    })
                    .reduce(0L, Long::sum);
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
