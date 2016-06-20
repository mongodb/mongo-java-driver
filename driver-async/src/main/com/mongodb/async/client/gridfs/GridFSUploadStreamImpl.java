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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.HexUtils.toHex;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;

final class GridFSUploadStreamImpl implements GridFSUploadStream {
    private static final Logger LOGGER = Loggers.getLogger("client.gridfs");
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<Document> chunksCollection;
    private final BsonValue fileId;
    private final String filename;
    private final int chunkSizeBytes;
    private final Document metadata;
    private final MessageDigest md5;
    private final GridFSIndexCheck indexCheck;
    private final Object closeAndWritingLock = new Object();


    /* protected by `closeAndWritingLock` */
    private boolean checkedIndexes;
    private boolean writing;
    private boolean closed;
    /* protected by `closeAndWritingLock` */

    /* accessed only when writing */
    private byte[] buffer;
    private long lengthInBytes;
    private int bufferOffset;
    private int chunkIndex;
    /* accessed only when writing */

    GridFSUploadStreamImpl(final MongoCollection<GridFSFile> filesCollection, final MongoCollection<Document> chunksCollection,
                           final BsonValue fileId, final String filename, final int chunkSizeBytes, final Document metadata,
                           final GridFSIndexCheck indexCheck) {
        this.filesCollection = notNull("files collection", filesCollection);
        this.chunksCollection = notNull("chunks collection", chunksCollection);
        this.fileId = notNull("File Id", fileId);
        this.filename = notNull("filename", filename);
        this.chunkSizeBytes = chunkSizeBytes;
        this.metadata = metadata;
        this.indexCheck = indexCheck;
        md5 = getDigest();
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
    public void abort(final SingleResultCallback<Void> callback) {
        notNull("callback", callback);
        final SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        if (!takeWritingLock(errHandlingCallback)) {
            return;
        }
        chunksCollection.deleteMany(new Document("files_id", fileId), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                releaseWritingLock();
                errHandlingCallback.onResult(null, t);
            }
        });
    }

    @Override
    public void write(final ByteBuffer src, final SingleResultCallback<Integer> callback) {
        notNull("src", src);
        notNull("callback", callback);
        final SingleResultCallback<Integer> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        boolean checkIndexes = false;
        synchronized (closeAndWritingLock) {
            checkIndexes = !checkedIndexes;
        }

        if (checkIndexes) {
            if (!takeWritingLock(errHandlingCallback)) {
                return;
            }
            indexCheck.checkAndCreateIndex(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    synchronized (closeAndWritingLock) {
                        checkedIndexes = true;
                    }
                    releaseWritingLock();
                    if (t != null) {
                        errHandlingCallback.onResult(null, t);
                    } else {
                        write(src, errHandlingCallback);
                    }
                }
            });
        } else {
            write(src.remaining() == 0 ? -1 : src.remaining(), src, errHandlingCallback);
        }
    }

    @Override
    public void close(final SingleResultCallback<Void> callback) {
        notNull("callback", callback);
        final SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        boolean alreadyClosed = false;
        synchronized (closeAndWritingLock) {
            alreadyClosed = closed;
            closed = true;
        }
        if (alreadyClosed) {
            errHandlingCallback.onResult(null, null);
            return;
        } else if (!getAndSetWritingLock()) {
            callbackIsWritingException(errHandlingCallback);
            return;
        }
        writeChunk(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    releaseWritingLock();
                    errHandlingCallback.onResult(null, t);
                } else {
                    GridFSFile gridFSFile = new GridFSFile(fileId, filename, lengthInBytes, chunkSizeBytes, new Date(),
                            toHex(md5.digest()), metadata);

                    filesCollection.insertOne(gridFSFile, new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            buffer = null;
                            releaseWritingLock();
                            errHandlingCallback.onResult(result, t);
                        }
                    });
                }
            }
        });
    }

    private void write(final int amount, final ByteBuffer src, final SingleResultCallback<Integer> callback) {
        if (!takeWritingLock(callback)){
            return;
        }

        int len = src.remaining();
        if (len == 0) {
            releaseWritingLock();
            callback.onResult(amount, null);
            return;
        }

        int amountToCopy = len;
        if (amountToCopy > chunkSizeBytes - bufferOffset) {
            amountToCopy = chunkSizeBytes - bufferOffset;
        }

        src.get(buffer, bufferOffset, amountToCopy);
        bufferOffset += amountToCopy;
        lengthInBytes += amountToCopy;
        if (bufferOffset == chunkSizeBytes) {
            writeChunk(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    releaseWritingLock();
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        write(amount, src, callback);
                    }
                }
            });
        } else {
            releaseWritingLock();
            callback.onResult(amount, null);
        }
    }

    private <T> boolean takeWritingLock(final SingleResultCallback<T> errHandlingCallback) {
        if (checkClosed()) {
            callbackClosedException(errHandlingCallback);
            return false;
        } else if (!getAndSetWritingLock()) {
            releaseWritingLock();
            callbackIsWritingException(errHandlingCallback);
            return false;
        }
        return true;
    }

    private void writeChunk(final SingleResultCallback<Void> callback) {
        if (md5 == null) {
            callback.onResult(null, new MongoGridFSException("No MD5 message digest available, cannot upload file"));
        } else if (bufferOffset > 0) {
            chunksCollection.insertOne(new Document("files_id", fileId).append("n", chunkIndex).append("data", getData()),
                    new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                md5.update(buffer);
                                chunkIndex++;
                                bufferOffset = 0;
                                callback.onResult(null, null);
                            }
                        }
                    }
            );
        } else {
            callback.onResult(null, null);
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

    private boolean checkClosed() {
        synchronized (closeAndWritingLock) {
            return closed;
        }
    }

    private boolean getAndSetWritingLock() {
        boolean gotLock = false;
        synchronized (closeAndWritingLock) {
            if (!writing) {
                writing = true;
                gotLock = true;
            }
        }
        return gotLock;
    }

    private void releaseWritingLock() {
        synchronized (closeAndWritingLock) {
            writing = false;
        }
    }

    private <T> void callbackClosedException(final SingleResultCallback<T> callback) {
        callback.onResult(null, new MongoGridFSException("The AsyncOutputStream has been closed"));
    }

    private <T> void callbackIsWritingException(final SingleResultCallback<T> callback) {
        callback.onResult(null, new MongoGridFSException("The AsyncOutputStream does not support concurrent writing."));
    }

    private static MessageDigest getDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }
}
