/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb.async.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.gridfs.model.GridFSFile;
import org.bson.Document;
import org.bson.types.Binary;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;

final class GridFSDownloadStreamImpl implements GridFSDownloadStream {
    private final GridFSFindIterable fileInfoIterable;
    private final MongoCollection<Document> chunksCollection;
    private final ConcurrentLinkedQueue<Document> resultsQueue = new ConcurrentLinkedQueue<Document>();

    private final Object closeAndReadingLock = new Object();

    /* protected by `closeAndReadingLock` */
    private boolean reading;
    private boolean closed;
    /* protected by `closeAndReadingLock` */

    private GridFSFile fileInfo;
    private int numberOfChunks;

    private AsyncBatchCursor<Document> cursor;
    private int batchSize;
    private int chunkIndex;
    private int bufferOffset;
    private long currentPosition;
    private byte[] buffer = null;


    GridFSDownloadStreamImpl(final GridFSFindIterable fileInfoIterable, final MongoCollection<Document> chunksCollection) {
        this.fileInfoIterable = notNull("file information", fileInfoIterable);
        this.chunksCollection = notNull("chunks collection", chunksCollection);
    }

    @Override
    public void getGridFSFile(final SingleResultCallback<GridFSFile> callback) {
        notNull("callback", callback);
        final SingleResultCallback<GridFSFile> errorHandlingCallback = errorHandlingCallback(callback);
        if (hasFileInfo()) {
            errorHandlingCallback.onResult(fileInfo, null);
            return;
        }

        if (!tryGetReadingLock(errorHandlingCallback)) {
            return;
        }

        fileInfoIterable.first(new SingleResultCallback<GridFSFile>() {
            @Override
            public void onResult(final GridFSFile result, final Throwable t) {
                releaseReadingLock();
                if (t != null) {
                    errorHandlingCallback.onResult(null, t);
                } else if (result == null) {
                    errorHandlingCallback.onResult(null, new MongoGridFSException("File not found"));
                } else {
                    fileInfo = result;
                    numberOfChunks = (int) Math.ceil((double) fileInfo.getLength() / fileInfo.getChunkSize());
                    errorHandlingCallback.onResult(result, null);
                }
            }
        });
    }

    @Override
    public GridFSDownloadStream batchSize(final int batchSize) {
        isTrueArgument("batchSize cannot be negative", batchSize >= 0);
        this.batchSize = batchSize;
        discardCursor();
        return this;
    }

    @Override
    public void read(final ByteBuffer dst, final SingleResultCallback<Integer> callback) {
        notNull("dst", dst);
        notNull("callback", callback);
        final SingleResultCallback<Integer> errorHandlingCallback = errorHandlingCallback(callback);
        if (!hasFileInfo()) {
            getGridFSFile(new SingleResultCallback<GridFSFile>() {
                @Override
                public void onResult(final GridFSFile result, final Throwable t) {
                    if (t != null) {
                        errorHandlingCallback.onResult(null, t);
                    } else {
                        read(dst, errorHandlingCallback);
                    }
                }
            });
            return;
        }

        if (!tryGetReadingLock(errorHandlingCallback)) {
            return;
        } else if (currentPosition == fileInfo.getLength()) {
            releaseReadingLock();
            callback.onResult(-1, null);
            return;
        }

        int amountToRead = dst.remaining();
        if (fileInfo.getLength() < amountToRead) {
            amountToRead = (int) fileInfo.getLength();
        }
        read(amountToRead, dst, errorHandlingCallback);
    }

    private void read(final int amountRead, final ByteBuffer dst, final SingleResultCallback<Integer> callback) {
        if (currentPosition == fileInfo.getLength() || dst.remaining() == 0) {
            releaseReadingLock();
            callback.onResult(amountRead, null);
            return;
        }

        boolean fetchBuffer = false;
        if (buffer == null) {
            fetchBuffer = true;
        } else if (bufferOffset == buffer.length) {
            chunkIndex += 1;
            bufferOffset = 0;
            fetchBuffer = true;
        }

        final RecursiveReadCallback recursiveCallback = new RecursiveReadCallback(amountRead, dst, callback);
        if (fetchBuffer) {
            getBuffer(chunkIndex, new SingleResultCallback<byte[]>() {
                @Override
                public void onResult(final byte[] result, final Throwable t) {
                    if (t != null) {
                        releaseReadingLock();
                        callback.onResult(null, t);
                    } else {
                        buffer = result;
                        readFromBuffer(dst, recursiveCallback);
                    }
                }
            });
        } else {
            readFromBuffer(dst, recursiveCallback);
        }
    }

    private void readFromBuffer(final ByteBuffer dst, final SingleResultCallback<Void> callback) {
        int amountToCopy = dst.remaining();
        if (amountToCopy > buffer.length - bufferOffset) {
            amountToCopy = buffer.length - bufferOffset;
        }
        dst.put(buffer, bufferOffset, amountToCopy);
        bufferOffset += amountToCopy;
        currentPosition += amountToCopy;
        callback.onResult(null, null);
    }

    @Override
    public void close(final SingleResultCallback<Void> callback) {
        notNull("callback", callback);
        SingleResultCallback<Void> errorHandlingCallback = errorHandlingCallback(callback);
        if (checkClosed()) {
            errorHandlingCallback.onResult(null, null);
        } else if (!getReadingLock()) {
            callbackIsReadingException(callback);
        } else {
            synchronized (closeAndReadingLock) {
                if (!closed) {
                    closed = true;
                }
            }
            discardCursor();
            errorHandlingCallback.onResult(null, null);
        }
    }

    private boolean hasFileInfo() {
        boolean hasInfo = false;
        synchronized (closeAndReadingLock) {
            hasInfo = fileInfo != null;
        }
        return hasInfo;
    }

    private void getChunk(final int startChunkIndex, final SingleResultCallback<Document> callback) {
        if (resultsQueue.isEmpty()) {
            if (cursor == null) {
                chunksCollection.find(new Document("files_id", fileInfo.getId())
                        .append("n", new Document("$gte", startChunkIndex)))
                        .batchSize(batchSize).sort(new Document("n", 1))
                        .batchCursor(new SingleResultCallback<AsyncBatchCursor<Document>>() {
                            @Override
                            public void onResult(final AsyncBatchCursor<Document> result, final Throwable t) {
                                if (t != null) {
                                    callback.onResult(null, t);
                                } else if (result == null) {
                                    chunkNotFound(startChunkIndex, callback);
                                } else {
                                    cursor = result;
                                    getChunk(startChunkIndex, callback);
                                }
                            }
                        });
            } else {
                cursor.next(new SingleResultCallback<List<Document>>() {
                    @Override
                    public void onResult(final List<Document> result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else if (result == null || result.isEmpty()) {
                            chunkNotFound(startChunkIndex, callback);
                        } else {
                            resultsQueue.addAll(result);
                            if (batchSize == 1) {
                                discardCursor();
                            }
                            getChunk(startChunkIndex, callback);
                        }
                    }
                });
            }
        } else {
            callback.onResult(resultsQueue.poll(), null);
        }
    }

    private <T> void chunkNotFound(final int startChunkIndex, final SingleResultCallback<T> callback) {
        callback.onResult(null, new MongoGridFSException(format("Could not find file chunk for file_id: %s at chunk index %s.",
                fileInfo.getId(), startChunkIndex)));
    }

    private void getBufferFromChunk(final Document chunk, final int expectedChunkIndex, final SingleResultCallback<byte[]> callback) {
        if (chunk == null || chunk.getInteger("n") != expectedChunkIndex) {
            chunkNotFound(expectedChunkIndex, callback);
            return;
        } else if (!(chunk.get("data") instanceof Binary)) {
            callback.onResult(null, new MongoGridFSException("Unexpected data format for the chunk"));
            return;
        }
        byte[] data = chunk.get("data", Binary.class).getData();

        long expectedDataLength = 0;
        if (expectedChunkIndex + 1 == numberOfChunks) {
            expectedDataLength = fileInfo.getLength() - (expectedChunkIndex * (long) fileInfo.getChunkSize());
        } else {
            expectedDataLength = fileInfo.getChunkSize();
        }

        if (data.length != expectedDataLength) {
            callback.onResult(null, new MongoGridFSException(format("Chunk size data length is not the expected size. "
                    + "The size was %s for file_id: %s chunk index %s it should be %s bytes.", data.length, fileInfo.getId(),
                    expectedChunkIndex, expectedDataLength)));
        } else {
            callback.onResult(data, null);
        }
    }

    private void getBuffer(final int chunkIndexToFetch, final SingleResultCallback<byte[]> callback) {
        getChunk(chunkIndexToFetch, new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    getBufferFromChunk(result, chunkIndexToFetch, callback);
                }
            }
        });
    }

    private <A> boolean tryGetReadingLock(final SingleResultCallback<A> callback) {
        if (checkClosed()) {
            callbackClosedException(callback);
            return false;
        } else if (!getReadingLock()) {
            callbackIsReadingException(callback);
            return false;
        } else {
            return true;
        }
    }

    private boolean checkClosed() {
        synchronized (closeAndReadingLock) {
            return closed;
        }
    }

    private boolean getReadingLock() {
        boolean gotLock = false;
        synchronized (closeAndReadingLock) {
            if (!reading) {
                reading = true;
                gotLock = true;
            }
        }
        return gotLock;
    }

    private void releaseReadingLock() {
        synchronized (closeAndReadingLock) {
            reading = false;
        }
    }

    private void discardCursor() {
        synchronized (closeAndReadingLock) {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        }
    }

    private <T> void callbackClosedException(final SingleResultCallback<T> callback) {
        callback.onResult(null, new MongoGridFSException("The AsyncInputStream has been closed"));
    }

    private <T> void callbackIsReadingException(final SingleResultCallback<T> callback) {
        callback.onResult(null, new MongoGridFSException("The AsyncInputStream does not support concurrent reading."));
    }

    private class RecursiveReadCallback implements SingleResultCallback<Void> {
        private final int amountRead;
        private final ByteBuffer dst;
        private final SingleResultCallback<Integer> callback;

        public RecursiveReadCallback(final int amountRead, final ByteBuffer dst, final SingleResultCallback<Integer> callback) {
            this.amountRead = amountRead;
            this.dst = dst;
            this.callback = callback;
        }

        @Override
        public void onResult(final Void result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                read(amountRead, dst, callback);
            }
        }
    }
}
