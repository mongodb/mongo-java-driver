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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.internal.TimeoutHelper;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.Locks.withInterruptibleLock;

final class GridFSUploadStreamImpl extends GridFSUploadStream {
    public static final String TIMEOUT_MESSAGE = "The GridFS upload stream exceeded the timeout limit.";
    private final ClientSession clientSession;
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<BsonDocument> chunksCollection;
    private final BsonValue fileId;
    private final String filename;
    private final int chunkSizeBytes;
    private final Document metadata;
    private byte[] buffer;
    private long lengthInBytes;
    private int bufferOffset;
    private int chunkIndex;
    @Nullable
    private final Timeout timeout;
    private final ReentrantLock closeLock = new ReentrantLock();
    private boolean closed = false;

    GridFSUploadStreamImpl(@Nullable final ClientSession clientSession, final MongoCollection<GridFSFile> filesCollection,
                           final MongoCollection<BsonDocument> chunksCollection, final BsonValue fileId, final String filename,
                           final int chunkSizeBytes, @Nullable final Document metadata, @Nullable final Timeout timeout) {
        this.clientSession = clientSession;
        this.filesCollection = notNull("files collection", filesCollection);
        this.chunksCollection = notNull("chunks collection", chunksCollection);
        this.fileId = notNull("File Id", fileId);
        this.filename = notNull("filename", filename);
        this.chunkSizeBytes = chunkSizeBytes;
        this.metadata = metadata;
        chunkIndex = 0;
        bufferOffset = 0;
        buffer = new byte[chunkSizeBytes];
        this.timeout = timeout;
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
    public void abort() {
        withInterruptibleLock(closeLock, () -> {
            checkClosed();
            closed = true;
        });

        if (clientSession != null) {
            withNullableTimeout(chunksCollection, timeout)
                    .deleteMany(clientSession, new Document("files_id", fileId));
        } else {
            withNullableTimeout(chunksCollection, timeout)
                    .deleteMany(new Document("files_id", fileId));
        }
    }

    @Override
    public void write(final int b) {
        byte[] byteArray = new byte[1];
        byteArray[0] = (byte) (0xFF & b);
        write(byteArray, 0, 1);
    }

    @Override
    public void write(final byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
        checkClosed();
        checkTimeout();
        notNull("b", b);

        if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        int currentOffset = off;
        int lengthToWrite = len;
        int amountToCopy = 0;

        while (lengthToWrite > 0) {
            amountToCopy = lengthToWrite;
            if (amountToCopy > chunkSizeBytes - bufferOffset) {
                amountToCopy = chunkSizeBytes - bufferOffset;
            }
            System.arraycopy(b, currentOffset, buffer, bufferOffset, amountToCopy);

            bufferOffset += amountToCopy;
            currentOffset += amountToCopy;
            lengthToWrite -= amountToCopy;
            lengthInBytes += amountToCopy;

            if (bufferOffset == chunkSizeBytes) {
                writeChunk();
            }
        }
    }

    private void checkTimeout() {
        Timeout.onExistsAndExpired(timeout, () -> TimeoutContext.throwMongoTimeoutException(TIMEOUT_MESSAGE));
    }

    @Override
    public void close() {
        boolean alreadyClosed = withInterruptibleLock(closeLock, () -> {
            boolean prevClosed = closed;
            closed = true;
            return prevClosed;
        });
        if (alreadyClosed) {
            return;
        }
        writeChunk();
        GridFSFile gridFSFile = new GridFSFile(fileId, filename, lengthInBytes, chunkSizeBytes, new Date(),
                metadata);
        if (clientSession != null) {
            withNullableTimeout(filesCollection, timeout).insertOne(clientSession, gridFSFile);
        } else {
            withNullableTimeout(filesCollection, timeout).insertOne(gridFSFile);
        }
        buffer = null;
    }

    private void writeChunk() {
        if (bufferOffset > 0) {
            if (clientSession != null) {
                withNullableTimeout(chunksCollection, timeout)
                        .insertOne(clientSession, new BsonDocument("files_id", fileId)
                                .append("n", new BsonInt32(chunkIndex))
                                .append("data", getData()));
            } else {
                withNullableTimeout(chunksCollection, timeout)
                        .insertOne(new BsonDocument("files_id", fileId)
                                .append("n", new BsonInt32(chunkIndex))
                                .append("data", getData()));
            }
            chunkIndex++;
            bufferOffset = 0;
        }
    }

    private BsonBinary getData() {
        if (bufferOffset < chunkSizeBytes) {
            byte[] sizedBuffer = new byte[bufferOffset];
            System.arraycopy(buffer, 0, sizedBuffer, 0, bufferOffset);
            buffer = sizedBuffer;
        }
        return new BsonBinary(buffer);
    }

    private void checkClosed() {
        withInterruptibleLock(closeLock, () -> {
            if (closed) {
                throw new MongoGridFSException("The OutputStream has been closed");
            }
        });
    }

    private static <T> MongoCollection<T> withNullableTimeout(final MongoCollection<T> collection,
                                                             @Nullable final Timeout timeout) {
        return TimeoutHelper.collectionWithTimeout(collection, TIMEOUT_MESSAGE, timeout);
    }
}
