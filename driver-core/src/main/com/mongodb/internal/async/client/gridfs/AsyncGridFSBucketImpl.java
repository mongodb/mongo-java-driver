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

package com.mongodb.internal.async.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.client.AsyncClientSession;
import com.mongodb.internal.async.client.AsyncFindIterable;
import com.mongodb.internal.async.client.AsyncMongoClients;
import com.mongodb.internal.async.client.AsyncMongoCollection;
import com.mongodb.internal.async.client.AsyncMongoDatabase;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


final class AsyncGridFSBucketImpl implements AsyncGridFSBucket {
    private static final Logger LOGGER = Loggers.getLogger("client.gridfs");
    private static final int DEFAULT_CHUNKSIZE_BYTES = 255 * 1024;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 4;
    private final String bucketName;
    private final int chunkSizeBytes;
    private final AsyncMongoCollection<GridFSFile> filesCollection;
    private final AsyncMongoCollection<Document> chunksCollection;

    AsyncGridFSBucketImpl(final AsyncMongoDatabase database) {
        this(database, "fs");
    }

    AsyncGridFSBucketImpl(final AsyncMongoDatabase database, final String bucketName) {
        this(notNull("bucketName", bucketName), DEFAULT_CHUNKSIZE_BYTES,
                getFilesCollection(notNull("database", database), bucketName),
                getChunksCollection(database, bucketName));
    }

    AsyncGridFSBucketImpl(final String bucketName, final int chunkSizeBytes, final AsyncMongoCollection<GridFSFile> filesCollection,
                          final AsyncMongoCollection<Document> chunksCollection) {
        this.bucketName = notNull("bucketName", bucketName);
        this.chunkSizeBytes = chunkSizeBytes;
        this.filesCollection = notNull("filesCollection", filesCollection);
        this.chunksCollection = notNull("chunksCollection", chunksCollection);
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public int getChunkSizeBytes() {
        return chunkSizeBytes;
    }

    @Override
    public ReadPreference getReadPreference() {
        return filesCollection.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return filesCollection.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return filesCollection.getReadConcern();
    }

    @Override
    public AsyncGridFSBucket withChunkSizeBytes(final int chunkSizeBytes) {
        return new AsyncGridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection, chunksCollection);
    }

    @Override
    public AsyncGridFSBucket withReadPreference(final ReadPreference readPreference) {
        notNull("readPreference", readPreference);
        return new AsyncGridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withReadPreference(readPreference),
                chunksCollection.withReadPreference(readPreference));
    }

    @Override
    public AsyncGridFSBucket withWriteConcern(final WriteConcern writeConcern) {
        notNull("writeConcern", writeConcern);
        return new AsyncGridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withWriteConcern(writeConcern),
                chunksCollection.withWriteConcern(writeConcern));
    }

    @Override
    public AsyncGridFSBucket withReadConcern(final ReadConcern readConcern) {
        notNull("readConcern", readConcern);
        return new AsyncGridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withReadConcern(readConcern),
                chunksCollection.withReadConcern(readConcern));
    }

    @Override
    public AsyncGridFSUploadStream openUploadStream(final String filename) {
        return openUploadStream(new BsonObjectId(), filename);
    }

    @Override
    public AsyncGridFSUploadStream openUploadStream(final String filename, final GridFSUploadOptions options) {
        return openUploadStream(new BsonObjectId(), filename, options);
    }

    @Override
    public AsyncGridFSUploadStream openUploadStream(final BsonValue id, final String filename) {
        return openUploadStream(id, filename, new GridFSUploadOptions());
    }

    @Override
    public AsyncGridFSUploadStream openUploadStream(final BsonValue id, final String filename, final GridFSUploadOptions options) {
        return createGridFSUploadStream(null, id, filename, options);
    }

    @Override
    public AsyncGridFSUploadStream openUploadStream(final AsyncClientSession clientSession, final String filename) {
        return openUploadStream(clientSession, new BsonObjectId(), filename);
    }

    @Override
    public AsyncGridFSUploadStream openUploadStream(final AsyncClientSession clientSession, final String filename,
                                                    final GridFSUploadOptions options) {
        return openUploadStream(clientSession, new BsonObjectId(), filename, options);
    }

    @Override
    public AsyncGridFSUploadStream openUploadStream(final AsyncClientSession clientSession, final BsonValue id, final String filename) {
        return openUploadStream(clientSession, id, filename, new GridFSUploadOptions());
    }

    @Override
    public AsyncGridFSUploadStream openUploadStream(final AsyncClientSession clientSession, final BsonValue id, final String filename,
                                                    final GridFSUploadOptions options) {
        notNull("clientSession", clientSession);
        return createGridFSUploadStream(clientSession, id, filename, options);
    }

    private AsyncGridFSUploadStream createGridFSUploadStream(@Nullable final AsyncClientSession clientSession, final BsonValue id,
                                                             final String filename, final GridFSUploadOptions options) {
        notNull("options", options);
        Integer chunkSizeBytes = options.getChunkSizeBytes();
        int chunkSize = chunkSizeBytes == null ? this.chunkSizeBytes : chunkSizeBytes;
        return new AsyncGridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, id, filename, chunkSize,
                options.getMetadata(), new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection));
    }

    @Override
    public void uploadFromStream(final String filename, final AsyncInputStream source, final SingleResultCallback<ObjectId> callback) {
        uploadFromStream(filename, source, new GridFSUploadOptions(), callback);
    }

    @Override
    public void uploadFromStream(final String filename, final AsyncInputStream source, final GridFSUploadOptions options,
                                 final SingleResultCallback<ObjectId> callback) {
        final BsonObjectId id = new BsonObjectId();
        uploadFromStream(id, filename, source, options, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(id.getValue(), null);
                }
            }
        });
    }

    @Override
    public void uploadFromStream(final BsonValue id, final String filename, final AsyncInputStream source,
                                 final SingleResultCallback<Void> callback) {
        uploadFromStream(id, filename, source, new GridFSUploadOptions(), callback);
    }

    @Override
    public void uploadFromStream(final BsonValue id, final String filename, final AsyncInputStream source,
                                 final GridFSUploadOptions options, final SingleResultCallback<Void> callback) {
        executeUploadFromStream(null, id, filename, source, options, callback);
    }

    @Override
    public void uploadFromStream(final AsyncClientSession clientSession, final String filename, final AsyncInputStream source,
                                 final SingleResultCallback<ObjectId> callback) {
        uploadFromStream(clientSession, filename, source, new GridFSUploadOptions(), callback);
    }

    @Override
    public void uploadFromStream(final AsyncClientSession clientSession, final String filename, final AsyncInputStream source,
                                 final GridFSUploadOptions options, final SingleResultCallback<ObjectId> callback) {
        final BsonObjectId id = new BsonObjectId();
        uploadFromStream(clientSession, id, filename, source, options, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(id.getValue(), null);
                }
            }
        });
    }

    @Override
    public void uploadFromStream(final AsyncClientSession clientSession, final BsonValue id, final String filename,
                                 final AsyncInputStream source, final SingleResultCallback<Void> callback) {
        uploadFromStream(clientSession, id, filename, source, new GridFSUploadOptions(), callback);
    }

    @Override
    public void uploadFromStream(final AsyncClientSession clientSession, final BsonValue id, final String filename,
                                 final AsyncInputStream source, final GridFSUploadOptions options,
                                 final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeUploadFromStream(clientSession, id, filename, source, options, callback);
    }

    private void executeUploadFromStream(@Nullable final AsyncClientSession clientSession, final BsonValue id, final String filename,
                                         final AsyncInputStream source, final GridFSUploadOptions options,
                                         final SingleResultCallback<Void> callback) {
        notNull("filename", filename);
        notNull("source", source);
        notNull("options", options);
        notNull("callback", callback);
        Integer chunkSizeBytes = options.getChunkSizeBytes();
        int chunkSize = chunkSizeBytes == null ? this.chunkSizeBytes : chunkSizeBytes;
        AsyncGridFSUploadStream uploadStream;
        if (clientSession != null){
            uploadStream = openUploadStream(clientSession, id, filename, options);
        } else {
            uploadStream = openUploadStream(id, filename, options);
        }
        readAndWriteInputStream(source, uploadStream, ByteBuffer.allocate(chunkSize), errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public AsyncGridFSDownloadStream openDownloadStream(final ObjectId id) {
        notNull("id", id);
        return createGridFSDownloadStream(null, find(new Document("_id", id)));
    }
    @Override
    public AsyncGridFSDownloadStream openDownloadStream(final BsonValue id) {
        notNull("id", id);
        return createGridFSDownloadStream(null, find(new Document("_id", id)));
    }

    @Override
    public AsyncGridFSDownloadStream openDownloadStream(final String filename) {
        return openDownloadStream(filename, new GridFSDownloadOptions());
    }

    @Override
    public AsyncGridFSDownloadStream openDownloadStream(final String filename, final GridFSDownloadOptions options) {
        return createGridFSDownloadStream(null, createGridFSFindIterable(null, filename, options));
    }

    @Override
    public AsyncGridFSDownloadStream openDownloadStream(final AsyncClientSession clientSession, final ObjectId id) {
        notNull("id", id);
        return createGridFSDownloadStream(clientSession, find(clientSession, new Document("_id", id)));
    }

    @Override
    public AsyncGridFSDownloadStream openDownloadStream(final AsyncClientSession clientSession, final BsonValue id) {
        notNull("id", id);
        return createGridFSDownloadStream(clientSession, find(clientSession, new Document("_id", id)));
    }

    @Override
    public AsyncGridFSDownloadStream openDownloadStream(final AsyncClientSession clientSession, final String filename) {
        return openDownloadStream(clientSession, filename, new GridFSDownloadOptions());
    }

    @Override
    public AsyncGridFSDownloadStream openDownloadStream(final AsyncClientSession clientSession, final String filename,
                                                        final GridFSDownloadOptions options) {
        notNull("clientSession", clientSession);
        return createGridFSDownloadStream(clientSession, createGridFSFindIterable(clientSession, filename, options));
    }

    private AsyncGridFSDownloadStream createGridFSDownloadStream(@Nullable final AsyncClientSession clientSession,
                                                                 final AsyncGridFSFindIterable gridFSFindIterable) {
        return new AsyncGridFSDownloadStreamImpl(clientSession, gridFSFindIterable, chunksCollection);
    }

    @Override
    public void downloadToStream(final String filename, final AsyncOutputStream destination, final SingleResultCallback<Long> callback) {
        downloadToStream(filename, destination, new GridFSDownloadOptions(), callback);
    }

    @Override
    public void downloadToStream(final String filename, final AsyncOutputStream destination,
                                 final GridFSDownloadOptions options, final SingleResultCallback<Long> callback) {
        downloadToAsyncOutputStream(openDownloadStream(filename, options), destination, errorHandlingCallback(callback, LOGGER));
    }


    @Override
    public void downloadToStream(final ObjectId id, final AsyncOutputStream destination, final SingleResultCallback<Long> callback) {
        downloadToAsyncOutputStream(openDownloadStream(id), destination, errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public void downloadToStream(final BsonValue id, final AsyncOutputStream destination, final SingleResultCallback<Long> callback) {
        downloadToAsyncOutputStream(openDownloadStream(id), destination, errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public void downloadToStream(final AsyncClientSession clientSession, final ObjectId id, final AsyncOutputStream destination,
                                 final SingleResultCallback<Long> callback) {
        downloadToAsyncOutputStream(openDownloadStream(clientSession, id), destination, errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public void downloadToStream(final AsyncClientSession clientSession, final BsonValue id, final AsyncOutputStream destination,
                                 final SingleResultCallback<Long> callback) {
        downloadToAsyncOutputStream(openDownloadStream(clientSession, id), destination, errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public void downloadToStream(final AsyncClientSession clientSession, final String filename, final AsyncOutputStream destination,
                                 final SingleResultCallback<Long> callback) {
        downloadToStream(clientSession, filename, destination, new GridFSDownloadOptions(), callback);
    }

    @Override
    public void downloadToStream(final AsyncClientSession clientSession, final String filename, final AsyncOutputStream destination,
                                 final GridFSDownloadOptions options, final SingleResultCallback<Long> callback) {
        notNull("clientSession", clientSession);
        downloadToAsyncOutputStream(openDownloadStream(clientSession, filename, options), destination,
                errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public AsyncGridFSFindIterable find() {
        return createGridFSFindIterable(null, null);
    }

    @Override
    public AsyncGridFSFindIterable find(final Bson filter) {
        notNull("filter", filter);
        return createGridFSFindIterable(null, filter);
    }

    @Override
    public AsyncGridFSFindIterable find(final AsyncClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createGridFSFindIterable(clientSession, null);
    }

    @Override
    public AsyncGridFSFindIterable find(final AsyncClientSession clientSession, final Bson filter) {
        notNull("clientSession", clientSession);
        notNull("filter", filter);
        return createGridFSFindIterable(clientSession, filter);
    }

    @Override
    public void delete(final ObjectId id, final SingleResultCallback<Void> callback) {
        delete(new BsonObjectId(id), callback);
    }

    @Override
    public void delete(final BsonValue id, final SingleResultCallback<Void> callback) {
        executeDelete(null, id, callback);
    }

    @Override
    public void delete(final AsyncClientSession clientSession, final ObjectId id, final SingleResultCallback<Void> callback) {
        delete(clientSession, new BsonObjectId(id), callback);
    }

    @Override
    public void delete(final AsyncClientSession clientSession, final BsonValue id, final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeDelete(clientSession, id, callback);
    }

   private void executeDelete(@Nullable final AsyncClientSession clientSession, final BsonValue id,
                              final SingleResultCallback<Void> callback) {
       notNull("id", id);
       notNull("callback", callback);
       final SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
       SingleResultCallback<DeleteResult> deleteFileCallback = new SingleResultCallback<DeleteResult>() {
           @Override
           public void onResult(final DeleteResult filesResult, final Throwable t) {
               if (t != null) {
                   errHandlingCallback.onResult(null, t);
               } else {
                   SingleResultCallback<DeleteResult> deleteChunksCallback = new SingleResultCallback<DeleteResult>() {
                       @Override
                       public void onResult(final DeleteResult chunksResult, final Throwable t) {
                           if (t != null) {
                               errHandlingCallback.onResult(null, t);
                           } else if (filesResult.wasAcknowledged() && filesResult.getDeletedCount() == 0) {
                               errHandlingCallback.onResult(null,
                                       new MongoGridFSException(format("No file found with the ObjectId: %s", id)));
                           } else {
                               errHandlingCallback.onResult(null, null);
                           }
                       }
                   };
                   if (clientSession != null) {
                       chunksCollection.deleteMany(clientSession, new BsonDocument("files_id", id), deleteChunksCallback);
                   } else {
                       chunksCollection.deleteMany(new BsonDocument("files_id", id), deleteChunksCallback);
                   }
               }
           }
       };

       if (clientSession != null) {
           filesCollection.deleteOne(clientSession, new BsonDocument("_id", id), deleteFileCallback);
       } else {
           filesCollection.deleteOne(new BsonDocument("_id", id), deleteFileCallback);
       }
   }

    @Override
    public void rename(final ObjectId id, final String newFilename, final SingleResultCallback<Void> callback) {
        rename(new BsonObjectId(id), newFilename, callback);
    }

    @Override
    public void rename(final BsonValue id, final String newFilename, final SingleResultCallback<Void> callback) {
        executeRename(null, id, newFilename, callback);
    }

    @Override
    public void rename(final AsyncClientSession clientSession, final ObjectId id, final String newFilename,
                       final SingleResultCallback<Void> callback) {
        rename(clientSession, new BsonObjectId(id), newFilename, callback);
    }

    @Override
    public void rename(final AsyncClientSession clientSession, final BsonValue id, final String newFilename,
                       final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeRename(clientSession, id, newFilename, callback);
    }

    private void executeRename(@Nullable final AsyncClientSession clientSession, final BsonValue id, final String newFilename,
                               final SingleResultCallback<Void> callback) {
        notNull("id", id);
        notNull("newFilename", newFilename);
        notNull("callback", callback);

        final SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        SingleResultCallback<UpdateResult> resultCallback = new SingleResultCallback<UpdateResult>() {
            @Override
            public void onResult(final UpdateResult result, final Throwable t) {
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
                    errHandlingCallback.onResult(null, new MongoGridFSException(format("No file found with the ObjectId: %s", id)));
                } else {
                    errHandlingCallback.onResult(null, null);
                }
            }
        };

        if (clientSession != null) {
            filesCollection.updateOne(clientSession, new BsonDocument("_id", id), new BsonDocument("$set",
                    new BsonDocument("filename", new BsonString(newFilename))), resultCallback);
        } else {
            filesCollection.updateOne(new BsonDocument("_id", id), new BsonDocument("$set",
                    new BsonDocument("filename", new BsonString(newFilename))), resultCallback);
        }
    }

    @Override
    public void drop(final SingleResultCallback<Void> callback) {
        executeDrop(null, callback);
    }

    @Override
    public void drop(final AsyncClientSession clientSession, final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeDrop(clientSession, callback);
    }

    private void executeDrop(@Nullable final AsyncClientSession clientSession, final SingleResultCallback<Void> callback) {
        notNull("callback", callback);
        final SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        SingleResultCallback<Void> dropFileCallback = new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    if (clientSession != null) {
                        chunksCollection.drop(clientSession, errHandlingCallback);
                    } else {
                        chunksCollection.drop(errHandlingCallback);
                    }
                }
            }
        };
        if (clientSession != null) {
            filesCollection.drop(clientSession, dropFileCallback);
        } else {
            filesCollection.drop(dropFileCallback);
        }
    }

    private AsyncGridFSFindIterable createGridFSFindIterable(@Nullable final AsyncClientSession clientSession,
                                                             @Nullable final Bson filter) {
        return new AsyncGridFSFindIterableImpl(createFindIterable(clientSession, filter));
    }

    private AsyncGridFSFindIterable createGridFSFindIterable(@Nullable final AsyncClientSession clientSession, final String filename,
                                                             final GridFSDownloadOptions options) {
        int revision = options.getRevision();
        int skip;
        int sort;
        if (revision >= 0) {
            skip = revision;
            sort = 1;
        } else {
            skip = (-revision) - 1;
            sort = -1;
        }

        return createGridFSFindIterable(clientSession, new Document("filename", filename)).skip(skip)
                .sort(new Document("uploadDate", sort));
    }

    private AsyncFindIterable<GridFSFile> createFindIterable(@Nullable final AsyncClientSession clientSession,
                                                             @Nullable final Bson filter) {
        AsyncFindIterable<GridFSFile> findIterable;
        if (clientSession != null) {
            findIterable = filesCollection.find(clientSession);
        } else {
            findIterable = filesCollection.find();
        }
        if (filter != null) {
            findIterable = findIterable.filter(filter);
        }
        return findIterable;
    }

    private static AsyncMongoCollection<GridFSFile> getFilesCollection(final AsyncMongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".files", GridFSFile.class).withCodecRegistry(
                fromRegistries(database.getCodecRegistry(), AsyncMongoClients.getDefaultCodecRegistry())
        );
    }

    private static AsyncMongoCollection<Document> getChunksCollection(final AsyncMongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".chunks").withCodecRegistry(AsyncMongoClients.getDefaultCodecRegistry());
    }

    private void downloadToAsyncOutputStream(final AsyncGridFSDownloadStream downloadStream, final AsyncOutputStream destination,
                                             final SingleResultCallback<Long> callback) {
        notNull("downloadStream", downloadStream);
        downloadStream.getGridFSFile(new SingleResultCallback<GridFSFile>() {
            @Override
            public void onResult(final GridFSFile result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    int bufferSize = DEFAULT_BUFFER_SIZE > result.getLength() ? (int) result.getLength() : DEFAULT_BUFFER_SIZE;
                    readAndWriteOutputStream(destination, downloadStream, ByteBuffer.allocate(bufferSize), 0, callback);
                }
            }
        });
    }

    private void readAndWriteInputStream(final AsyncInputStream source,
                                         final AsyncGridFSUploadStream uploadStream, final ByteBuffer buffer,
                                         final SingleResultCallback<Void> callback) {
        ((Buffer) buffer).clear();
        source.read(buffer, new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final Throwable t) {
                if (t != null) {
                    if (t instanceof IOException) {
                        uploadStream.abort(new SingleResultCallback<Void>() {
                            @Override
                            public void onResult(final Void result, final Throwable abortException) {
                                if (abortException != null) {
                                    callback.onResult(null, abortException);
                                } else {
                                    callback.onResult(null, new MongoGridFSException("IOException when reading from the InputStream", t));
                                }
                            }
                        });
                    } else {
                        callback.onResult(null, t);
                    }
                } else if (result > 0) {
                    ((Buffer) buffer).flip();
                    uploadStream.write(buffer, new SingleResultCallback<Integer>() {
                        @Override
                        public void onResult(final Integer result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                readAndWriteInputStream(source, uploadStream, buffer, callback);
                            }
                        }
                    });
                } else {
                    uploadStream.close(callback);
                }
            }
        });
    }

    private void readAndWriteOutputStream(final AsyncOutputStream destination, final AsyncGridFSDownloadStream downloadStream,
                                          final ByteBuffer buffer, final long amountRead, final SingleResultCallback<Long> callback) {
        ((Buffer) buffer).clear();
        downloadStream.read(buffer, new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer readResult, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else if (readResult > 0) {
                    ((Buffer) buffer).flip();
                    destination.write(buffer, new SingleResultCallback<Integer>() {
                        @Override
                        public void onResult(final Integer writeResult, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                readAndWriteOutputStream(destination, downloadStream, buffer, amountRead + writeResult, callback);
                            }
                        }
                    });
                } else {
                    callback.onResult(amountRead, null);
                }
            }
        });
    }

}
