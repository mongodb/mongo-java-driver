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

package com.mongodb.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.internal.TimeoutHelper;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;

import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.Locks.withInterruptibleLock;
import static com.mongodb.internal.TimeoutContext.createMongoTimeoutException;
import static java.lang.String.format;

class GridFSDownloadStreamImpl extends GridFSDownloadStream {
    private static final String TIMEOUT_MESSAGE = "The GridFS download stream exceeded the timeout limit.";
    private final ClientSession clientSession;
    private final GridFSFile fileInfo;
    private final MongoCollection<BsonDocument> chunksCollection;
    private final BsonValue fileId;
    /**
     * The length, in bytes of the file to download.
     */
    private final long length;
    private final int chunkSizeInBytes;
    private final int numberOfChunks;
    private MongoCursor<BsonDocument> cursor;
    private int batchSize;
    private int chunkIndex;
    private int bufferOffset;
    /**
     * Current byte position in the file.
     */
    private long currentPosition;
    private byte[] buffer = null;
    private long markPosition;
    @Nullable
    private final Timeout timeout;
    private final ReentrantLock closeLock = new ReentrantLock();
    private final ReentrantLock cursorLock = new ReentrantLock();
    private boolean closed = false;

    GridFSDownloadStreamImpl(@Nullable final ClientSession clientSession, final GridFSFile fileInfo,
                             final MongoCollection<BsonDocument> chunksCollection, @Nullable final Timeout timeout) {
        this.clientSession = clientSession;
        this.fileInfo = notNull("file information", fileInfo);
        this.chunksCollection = notNull("chunks collection",  chunksCollection);

        fileId = fileInfo.getId();
        length = fileInfo.getLength();
        chunkSizeInBytes = fileInfo.getChunkSize();
        numberOfChunks = (int) Math.ceil((double) length / chunkSizeInBytes);
        this.timeout = timeout;
    }

    @Override
    public GridFSFile getGridFSFile() {
        return fileInfo;
    }

    @Override
    public GridFSDownloadStream batchSize(final int batchSize) {
        isTrueArgument("batchSize cannot be negative", batchSize >= 0);
        this.batchSize = batchSize;
        discardCursor();
        return this;
    }

    @Override
    public int read() {
        byte[] b = new byte[1];
        int res = read(b);
        if (res < 0) {
            return -1;
        }
        return b[0] & 0xFF;
    }

    @Override
    public int read(final byte[] b) {
        return read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) {
        checkClosed();
        checkTimeout();

        if (currentPosition == length) {
            return -1;
        } else if (buffer == null) {
            buffer = getBuffer(chunkIndex);
        } else if (bufferOffset == buffer.length) {
            chunkIndex += 1;
            buffer = getBuffer(chunkIndex);
            bufferOffset = 0;
        }

        int r = Math.min(len, buffer.length - bufferOffset);
        System.arraycopy(buffer, bufferOffset, b, off, r);
        bufferOffset += r;
        currentPosition += r;
        return r;
    }

    @Override
    public long skip(final long bytesToSkip) {
        checkClosed();
        checkTimeout();
        if (bytesToSkip <= 0) {
            return 0;
        }

        long skippedPosition = currentPosition + bytesToSkip;
        bufferOffset = (int) (skippedPosition % chunkSizeInBytes);
        if (skippedPosition >= length) {
            long skipped = length - currentPosition;
            chunkIndex = numberOfChunks - 1;
            currentPosition = length;
            buffer = null;
            discardCursor();
            return skipped;
        } else {
            int newChunkIndex = (int) Math.floor(skippedPosition / (double) chunkSizeInBytes);
            if (chunkIndex != newChunkIndex) {
                chunkIndex = newChunkIndex;
                buffer = null;
                discardCursor();
            }
            currentPosition += bytesToSkip;
            return bytesToSkip;
        }
    }

    @Override
    public int available() {
        checkClosed();
        checkTimeout();
        if (buffer == null) {
            return 0;
        } else {
            return buffer.length - bufferOffset;
        }
    }

    @Override
    public void mark() {
        mark(Integer.MAX_VALUE);
    }

    @Override
    public void mark(final int readlimit) {
        markPosition = currentPosition;
    }

    @Override
    public void reset() {
        checkClosed();
        checkTimeout();
        if (currentPosition == markPosition) {
            return;
        }

        bufferOffset = (int) (markPosition % chunkSizeInBytes);
        currentPosition = markPosition;
        int markChunkIndex = (int) Math.floor(markPosition / (double) chunkSizeInBytes);
        if (markChunkIndex != chunkIndex) {
            chunkIndex = markChunkIndex;
            buffer = null;
            discardCursor();
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void close() {
        withInterruptibleLock(closeLock, () -> {
            if (!closed) {
                closed = true;
            }
            discardCursor();
        });
    }

    private void checkTimeout() {
        Timeout.onExistsAndExpired(timeout, () -> {
            throw createMongoTimeoutException(TIMEOUT_MESSAGE);
        });
    }
    private void checkClosed() {
        withInterruptibleLock(closeLock, () -> {
            if (closed) {
                throw new MongoGridFSException("The InputStream has been closed");
            }
        });
    }

    private void discardCursor() {
        withInterruptibleLock(cursorLock, () -> {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        });
    }

    @Nullable
    private BsonDocument getChunk(final int startChunkIndex) {
        if (cursor == null) {
            cursor = getCursor(startChunkIndex);
        }
        BsonDocument chunk = null;
        if (cursor.hasNext()) {
            chunk = cursor.next();
            if (batchSize == 1) {
                discardCursor();
            }
            if (chunk.getInt32("n").getValue() != startChunkIndex) {
                throw new MongoGridFSException(format("Could not find file chunk for file_id: %s at chunk index %s.",
                        fileId, startChunkIndex));
            }
        }

        return chunk;
    }

    private MongoCursor<BsonDocument> getCursor(final int startChunkIndex) {
        FindIterable<BsonDocument> findIterable;
        BsonDocument filter = new BsonDocument("files_id", fileId).append("n", new BsonDocument("$gte", new BsonInt32(startChunkIndex)));
        if (clientSession != null) {
            findIterable = withNullableTimeout(chunksCollection, timeout).find(clientSession, filter);
        } else {
            findIterable =  withNullableTimeout(chunksCollection, timeout).find(filter);
        }
        if (timeout != null){
             findIterable.timeoutMode(TimeoutMode.CURSOR_LIFETIME);
        }
        return findIterable.batchSize(batchSize)
                .sort(new BsonDocument("n", new BsonInt32(1))).iterator();
    }

    private byte[] getBufferFromChunk(@Nullable final BsonDocument chunk, final int expectedChunkIndex) {

        if (chunk == null || chunk.getInt32("n").getValue() != expectedChunkIndex) {
            throw new MongoGridFSException(format("Could not find file chunk for file_id: %s at chunk index %s.",
                    fileId, expectedChunkIndex));
        }

        if (!(chunk.get("data") instanceof BsonBinary)) {
            throw new MongoGridFSException("Unexpected data format for the chunk");
        }
        byte[] data = chunk.getBinary("data").getData();

        long expectedDataLength = 0;
        boolean extraChunk = false;
        if (expectedChunkIndex + 1 > numberOfChunks) {
            extraChunk = true;
        } else if (expectedChunkIndex + 1 == numberOfChunks) {
            expectedDataLength = length - (expectedChunkIndex * (long) chunkSizeInBytes);
        } else {
            expectedDataLength = chunkSizeInBytes;
        }

        if (extraChunk && data.length > expectedDataLength) {
            throw new MongoGridFSException(format("Extra chunk data for file_id: %s. Unexpected chunk at chunk index %s."
                    + "The size was %s and it should be %s bytes.", fileId, expectedChunkIndex, data.length, expectedDataLength));
        } else if (data.length != expectedDataLength) {
            throw new MongoGridFSException(format("Chunk size data length is not the expected size. "
                            + "The size was %s for file_id: %s chunk index %s it should be %s bytes.",
                    data.length, fileId, expectedChunkIndex, expectedDataLength));
        }
        return data;
    }

    private byte[] getBuffer(final int chunkIndexToFetch) {
        return getBufferFromChunk(getChunk(chunkIndexToFetch), chunkIndexToFetch);
    }

    private <T> MongoCollection<T> withNullableTimeout(final MongoCollection<T> chunksCollection,
                                                       @Nullable final Timeout timeout) {
        return TimeoutHelper.collectionWithTimeout(chunksCollection, TIMEOUT_MESSAGE, timeout);
    }
}
