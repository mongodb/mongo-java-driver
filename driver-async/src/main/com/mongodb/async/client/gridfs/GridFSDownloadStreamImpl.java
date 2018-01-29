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

package com.mongodb.async.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.session.ClientSession;
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
    private static final Logger LOGGER = Loggers.getLogger("client.gridfs");
    private final ClientSession clientSession;
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


    GridFSDownloadStreamImpl(final ClientSession clientSession, final GridFSFindIterable fileInfoIterable,
                             final MongoCollection<Document> chunksCollection) {
        this.clientSession = clientSession;
        this.fileInfoIterable = notNull("file information", fileInfoIterable);
        this.chunksCollection = notNull("chunks collection", chunksCollection);
    }

    @Override
    public void getGridFSFile(final SingleResultCallback<GridFSFile> callback) {
        notNull("callback", callback);
        final SingleResultCallback<GridFSFile> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        if (hasFileInfo()) {
            errHandlingCallback.onResult(fileInfo, null);
            return;
        }

        if (!tryGetReadingLock(errHandlingCallback)) {
            return;
        }

        fileInfoIterable.first(new SingleResultCallback<GridFSFile>() {
            @Override
            public void onResult(final GridFSFile result, final Throwable t) {
                releaseReadingLock();
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else if (result == null) {
                    errHandlingCallback.onResult(null, new MongoGridFSException("File not found"));
                } else {
                    fileInfo = result;
                    numberOfChunks = (int) Math.ceil((double) fileInfo.getLength() / fileInfo.getChunkSize());
                    errHandlingCallback.onResult(result, null);
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
        final SingleResultCallback<Integer> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        if (!hasFileInfo()) {
            getGridFSFile(new SingleResultCallback<GridFSFile>() {
                @Override
                public void onResult(final GridFSFile result, final Throwable t) {
                    if (t != null) {
                        errHandlingCallback.onResult(null, t);
                    } else {
                        read(dst, errHandlingCallback);
                    }
                }
            });
            return;
        }

        if (!tryGetReadingLock(errHandlingCallback)) {
            return;
        } else if (currentPosition == fileInfo.getLength()) {
            releaseReadingLock();
            errHandlingCallback.onResult(-1, null);
            return;
        }

        checkAndFetchResults(0, dst, new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final Throwable t) {
                releaseReadingLock();
                errHandlingCallback.onResult(result, t);
            }
        });
    }

    private void checkAndFetchResults(final int amountRead, final ByteBuffer dst, final SingleResultCallback<Integer> callback) {
        if (currentPosition == fileInfo.getLength() || dst.remaining() == 0) {
            callback.onResult(amountRead, null);
        } else if (hasResultsToProcess()) {
            processResults(amountRead, dst, callback);
        } else if (cursor == null) {
            Document filter = new Document("files_id", fileInfo.getId())
                    .append("n", new Document("$gte", chunkIndex));

            FindIterable<Document> findIterable;
            if (clientSession != null) {
                findIterable = chunksCollection.find(clientSession, filter);
            } else {
                findIterable = chunksCollection.find(filter);
            }
            findIterable.batchSize(batchSize).sort(new Document("n", 1)).batchCursor(
                    new SingleResultCallback<AsyncBatchCursor<Document>>() {
                        @Override
                        public void onResult(final AsyncBatchCursor<Document> result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                cursor = result;
                                checkAndFetchResults(amountRead, dst, callback);
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
                        callback.onResult(null, chunkNotFound(chunkIndex));
                    } else {
                        resultsQueue.addAll(result);
                        if (batchSize == 1) {
                            discardCursor();
                        }
                        processResults(amountRead, dst, callback);
                    }
                }
            });
        }
    }

    private void processResults(final int previousAmountRead, final ByteBuffer dst, final SingleResultCallback<Integer> callback) {
        try {
            int amountRead = previousAmountRead;
            int amountToCopy = dst.remaining();
            while (currentPosition < fileInfo.getLength() && amountToCopy > 0) {

                if (getBufferFromResultsQueue()) {
                    buffer = getBufferFromChunk(resultsQueue.poll(), chunkIndex);
                    bufferOffset = 0;
                    chunkIndex += 1;
                }

                if (amountToCopy > buffer.length - bufferOffset) {
                    amountToCopy = buffer.length - bufferOffset;
                }

                if (amountToCopy > 0) {
                    dst.put(buffer, bufferOffset, amountToCopy);
                    bufferOffset += amountToCopy;
                    currentPosition += amountToCopy;
                    amountRead += amountToCopy;
                    amountToCopy = dst.remaining();
                }
            }
            checkAndFetchResults(amountRead, dst, callback);
        } catch (MongoGridFSException e) {
            callback.onResult(null, e);
        }
    }

    @Override
    public void close(final SingleResultCallback<Void> callback) {
        notNull("callback", callback);
        SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        if (checkClosed()) {
            errHandlingCallback.onResult(null, null);
        } else if (!getReadingLock()) {
            callbackIsReadingException(callback);
        } else {
            synchronized (closeAndReadingLock) {
                if (!closed) {
                    closed = true;
                }
            }
            discardCursor();
            errHandlingCallback.onResult(null, null);
        }
    }

    private boolean hasFileInfo() {
        boolean hasInfo = false;
        synchronized (closeAndReadingLock) {
            hasInfo = fileInfo != null;
        }
        return hasInfo;
    }

    private MongoGridFSException chunkNotFound(final int chunkIndex) {
        return new MongoGridFSException(format("Could not find file chunk for files_id: %s at chunk index %s.", fileInfo.getId(),
                chunkIndex));
    }

    private byte[] getBufferFromChunk(final Document chunk, final int expectedChunkIndex) {
        if (chunk == null || chunk.getInteger("n") != expectedChunkIndex) {
            throw chunkNotFound(expectedChunkIndex);
        } else if (!(chunk.get("data") instanceof Binary)) {
            throw new MongoGridFSException("Unexpected data format for the chunk");
        }

        byte[] data = chunk.get("data", Binary.class).getData();

        long expectedDataLength =
                expectedChunkIndex + 1 == numberOfChunks
                        ? fileInfo.getLength() - (expectedChunkIndex * (long) fileInfo.getChunkSize())
                        : fileInfo.getChunkSize();

        if (data.length != expectedDataLength) {
            throw new MongoGridFSException(format("Chunk size data length is not the expected size. "
                            + "The size was %s for file_id: %s chunk index %s it should be %s bytes.", data.length, fileInfo.getId(),
                    expectedChunkIndex, expectedDataLength));
        }

        return data;
    }

    private boolean getBufferFromResultsQueue() {
        return !resultsQueue.isEmpty() && (buffer == null || bufferOffset == buffer.length);
    }

    private boolean hasResultsToProcess() {
        return !resultsQueue.isEmpty() || (buffer != null && bufferOffset < buffer.length);
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
}
