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

package com.mongodb.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.model.GridFSFile;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

class GridFSDownloadStreamImpl extends GridFSDownloadStream {
    private final GridFSFile fileInfo;
    private final MongoCollection<Document> chunksCollection;
    private final BsonValue fileId;
    private final long length;
    private final int chunkSizeInBytes;
    private final int numberOfChunks;
    private MongoCursor<Document> cursor;
    private int batchSize;
    private int chunkIndex;
    private int bufferOffset;
    private long currentPosition;
    private byte[] buffer = null;
    private long markPosition;

    private final Object closeLock = new Object();
    private final Object cursorLock = new Object();
    private boolean closed = false;

    GridFSDownloadStreamImpl(final GridFSFile fileInfo, final MongoCollection<Document> chunksCollection) {
        this.fileInfo = notNull("file information", fileInfo);
        this.chunksCollection = notNull("chunks collection", chunksCollection);

        fileId = fileInfo.getId();
        length = fileInfo.getLength();
        chunkSizeInBytes = fileInfo.getChunkSize();
        numberOfChunks = (int) Math.ceil((double) length / chunkSizeInBytes);
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
    public synchronized void mark(final int readlimit) {
        markPosition = currentPosition;
    }

    @Override
    public synchronized void reset() {
        checkClosed();
        if (currentPosition == markPosition) {
            return;
        }

        bufferOffset = (int) (markPosition % chunkSizeInBytes);
        currentPosition = markPosition;
        int markChunkIndex = (int) Math.floor(markPosition / (double) chunkSizeInBytes);
        if (markChunkIndex != chunkIndex) {
            chunkIndex = markChunkIndex;
            buffer = null;
            cursor = null;
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void close() {
        synchronized (closeLock) {
            if (!closed) {
                closed = true;
            }
            discardCursor();
        }
    }

    private void checkClosed() {
        synchronized (closeLock) {
            if (closed) {
                throw new MongoGridFSException("The InputStream has been closed");
            }
        }
    }

    private void discardCursor() {
        synchronized (cursorLock) {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        }
    }

    private Document getChunk(final int startChunkIndex) {
        if (cursor == null) {
            cursor = chunksCollection.find(new Document("files_id", fileId).append("n", new Document("$gte", startChunkIndex)))
                    .batchSize(batchSize).sort(new Document("n", 1)).iterator();
        }
        Document chunk = null;
        if (cursor.hasNext()) {
            chunk = cursor.next();
            if (batchSize == 1) {
                discardCursor();
            }
            if (chunk.getInteger("n") != startChunkIndex) {
                throw new MongoGridFSException(format("Could not find file chunk for file_id: %s at chunk index %s.",
                        fileId, startChunkIndex));
            }
        }

        return chunk;
    }

    private byte[] getBufferFromChunk(final Document chunk, final int expectedChunkIndex) {

        if (chunk == null || chunk.getInteger("n") != expectedChunkIndex) {
            throw new MongoGridFSException(format("Could not find file chunk for file_id: %s at chunk index %s.",
                    fileId, expectedChunkIndex));
        }

        if (!(chunk.get("data") instanceof Binary)) {
            throw new MongoGridFSException("Unexpected data format for the chunk");
        }
        byte[] data = chunk.get("data", Binary.class).getData();

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
}
