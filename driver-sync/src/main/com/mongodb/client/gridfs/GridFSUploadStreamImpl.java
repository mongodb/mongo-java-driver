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
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.util.Date;

import static com.mongodb.assertions.Assertions.notNull;

final class GridFSUploadStreamImpl extends GridFSUploadStream {
    private final ClientSession clientSession;
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<Document> chunksCollection;
    private final BsonValue fileId;
    private final String filename;
    private final int chunkSizeBytes;
    private final Document metadata;
    private byte[] buffer;
    private long lengthInBytes;
    private int bufferOffset;
    private int chunkIndex;

    private final Object closeLock = new Object();
    private boolean closed = false;

    GridFSUploadStreamImpl(@Nullable final ClientSession clientSession, final MongoCollection<GridFSFile> filesCollection,
                           final MongoCollection<Document> chunksCollection, final BsonValue fileId, final String filename,
                           final int chunkSizeBytes, @Nullable final Document metadata) {
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
        synchronized (closeLock) {
            checkClosed();
            closed = true;
        }
        if (clientSession != null) {
            chunksCollection.deleteMany(clientSession, new Document("files_id", fileId));
        } else {
            chunksCollection.deleteMany(new Document("files_id", fileId));
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

    @Override
    public void close() {
        synchronized (closeLock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        writeChunk();
        GridFSFile gridFSFile = new GridFSFile(fileId, filename, lengthInBytes, chunkSizeBytes, new Date(),
                metadata);
        if (clientSession != null) {
            filesCollection.insertOne(clientSession, gridFSFile);
        } else {
            filesCollection.insertOne(gridFSFile);
        }
        buffer = null;
    }

    private void writeChunk() {
        if (bufferOffset > 0) {
            if (clientSession != null) {
                chunksCollection.insertOne(clientSession, new Document("files_id", fileId).append("n", chunkIndex)
                        .append("data", getData()));
            } else {
                chunksCollection.insertOne(new Document("files_id", fileId).append("n", chunkIndex).append("data", getData()));
            }
            chunkIndex++;
            bufferOffset = 0;
        }
    }

    private Binary getData() {
        if (bufferOffset < chunkSizeBytes) {
            byte[] sizedBuffer = new byte[bufferOffset];
            System.arraycopy(buffer, 0, sizedBuffer, 0, bufferOffset);
            buffer = sizedBuffer;
        }
        return new Binary(buffer);
    }

    private void checkClosed() {
        synchronized (closeLock) {
            if (closed) {
                throw new MongoGridFSException("The OutputStream has been closed");
            }
        }
    }
}
