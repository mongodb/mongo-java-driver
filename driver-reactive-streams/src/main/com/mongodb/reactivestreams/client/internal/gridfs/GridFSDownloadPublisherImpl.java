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
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import org.bson.Document;
import org.bson.types.Binary;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.TimeoutContext.calculateTimeout;
import static com.mongodb.reactivestreams.client.internal.gridfs.TimeoutUtils.withNullableTimeout;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class GridFSDownloadPublisherImpl implements GridFSDownloadPublisher {
    private final ClientSession clientSession;
    private final Function<Timeout, GridFSFindPublisher> gridFSFileMono;
    private final MongoCollection<Document> chunksCollection;
    private Integer bufferSizeBytes;

    private volatile GridFSFile fileInfo;
    @Nullable
    private final Long timeoutMs;

    public GridFSDownloadPublisherImpl(@Nullable final ClientSession clientSession,
                                       final Function<Timeout, GridFSFindPublisher> gridFSFilePublisherCreator,
                                       final MongoCollection<Document> chunksCollection) {
        this.clientSession = clientSession;
        this.gridFSFileMono = notNull("gridFSFilePublisherCreator", gridFSFilePublisherCreator);
        this.chunksCollection = notNull("chunksCollection", chunksCollection);
        this.timeoutMs = chunksCollection.getTimeout(MILLISECONDS);
    }

    @Override
    public Publisher<GridFSFile> getGridFSFile() {
        if (fileInfo != null) {
            return Mono.fromCallable(() -> fileInfo);
        }
        return Mono.from(gridFSFileMono.apply(calculateTimeout(timeoutMs)))
                .doOnNext(gridFSFile -> fileInfo = gridFSFile);
    }

    @Override
    public GridFSDownloadPublisher bufferSizeBytes(final int bufferSizeBytes) {
        this.bufferSizeBytes = bufferSizeBytes;
        return this;
    }

    @Override
    public void subscribe(final Subscriber<? super ByteBuffer> subscriber) {
        Flux.defer(()-> {
            Timeout operationTimeout = calculateTimeout(timeoutMs);
           return Mono.from(gridFSFileMono.apply(operationTimeout))
                    .doOnSuccess(gridFSFile -> {
                        //TODO check if it works
                        if (gridFSFile == null) {
                            throw new MongoGridFSException("File not found");
                        }
                        fileInfo = gridFSFile;
                    }).flatMapMany((Function<GridFSFile, Flux<ByteBuffer>>) gridFSFile -> getChunkPublisher(gridFSFile, operationTimeout));
        }).subscribe(subscriber);
    }

    private Flux<ByteBuffer> getChunkPublisher(final GridFSFile gridFSFile, @Nullable final Timeout timeout) {
        //TODO check if async timeout works correctly here with no delays
        Document filter = new Document("files_id", gridFSFile.getId());
        FindPublisher<Document> chunkPublisher;
        if (clientSession != null) {
            chunkPublisher = withNullableTimeout(chunksCollection, timeout).find(clientSession, filter);
        } else {
            chunkPublisher = withNullableTimeout(chunksCollection, timeout).find(filter);
        }

        AtomicInteger chunkCounter = new AtomicInteger(0);
        int numberOfChunks = (int) Math.ceil((double) gridFSFile.getLength() / gridFSFile.getChunkSize());
        Flux<ByteBuffer> byteBufferFlux = Flux.from(chunkPublisher.sort(new Document("n", 1)))
                .map(chunk -> {
                    int expectedChunkIndex = chunkCounter.getAndAdd(1);
                    if (chunk == null || chunk.getInteger("n") != expectedChunkIndex) {
                        throw new MongoGridFSException(format("Could not find file chunk for files_id: %s at chunk index %s.",
                                                              gridFSFile.getId(), expectedChunkIndex));
                    } else if (!(chunk.get("data") instanceof Binary)) {
                        throw new MongoGridFSException("Unexpected data format for the chunk");
                    }

                    byte[] data = chunk.get("data", Binary.class).getData();

                    long expectedDataLength = 0;
                    if (numberOfChunks > 0) {
                        expectedDataLength = expectedChunkIndex + 1 == numberOfChunks
                                ? gridFSFile.getLength() - (expectedChunkIndex * (long) gridFSFile.getChunkSize())
                                : gridFSFile.getChunkSize();
                    }

                    if (data.length != expectedDataLength) {
                        throw new MongoGridFSException(format("Chunk size data length is not the expected size. "
                                                                      + "The size was %s for file_id: %s chunk index %s it should be "
                                                                      + "%s bytes.",
                                       data.length, gridFSFile.getId(), expectedChunkIndex, expectedDataLength));
                    }
                    return ByteBuffer.wrap(data);
                }).doOnComplete(() -> {
                    if (chunkCounter.get() < numberOfChunks) {
                        throw new MongoGridFSException(format("Could not find file chunk for files_id: %s at chunk index %s.",
                                                              gridFSFile.getId(), chunkCounter.get()));
                    }
                });
        return bufferSizeBytes == null ? byteBufferFlux : new ResizingByteBufferFlux(byteBufferFlux, bufferSizeBytes);
    }
}
