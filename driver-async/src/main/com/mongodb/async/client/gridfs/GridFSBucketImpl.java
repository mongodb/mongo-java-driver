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
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.session.ClientSession;
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


final class GridFSBucketImpl implements GridFSBucket {
    private static final Logger LOGGER = Loggers.getLogger("client.gridfs");
    private static final int DEFAULT_CHUNKSIZE_BYTES = 255 * 1024;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 4;
    private final String bucketName;
    private final int chunkSizeBytes;
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<Document> chunksCollection;

    GridFSBucketImpl(final MongoDatabase database) {
        this(database, "fs");
    }

    GridFSBucketImpl(final MongoDatabase database, final String bucketName) {
        this(notNull("bucketName", bucketName), DEFAULT_CHUNKSIZE_BYTES,
                getFilesCollection(notNull("database", database), bucketName),
                getChunksCollection(database, bucketName));
    }

    GridFSBucketImpl(final String bucketName, final int chunkSizeBytes, final MongoCollection<GridFSFile> filesCollection,
                     final MongoCollection<Document> chunksCollection) {
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
    public GridFSBucket withChunkSizeBytes(final int chunkSizeBytes) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection, chunksCollection);
    }

    @Override
    public GridFSBucket withReadPreference(final ReadPreference readPreference) {
        notNull("readPreference", readPreference);
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withReadPreference(readPreference),
                chunksCollection.withReadPreference(readPreference));
    }

    @Override
    public GridFSBucket withWriteConcern(final WriteConcern writeConcern) {
        notNull("writeConcern", writeConcern);
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withWriteConcern(writeConcern),
                chunksCollection.withWriteConcern(writeConcern));
    }

    @Override
    public GridFSBucket withReadConcern(final ReadConcern readConcern) {
        notNull("readConcern", readConcern);
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withReadConcern(readConcern),
                chunksCollection.withReadConcern(readConcern));
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename) {
        return openUploadStream(new BsonObjectId(), filename);
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename, final GridFSUploadOptions options) {
        return openUploadStream(new BsonObjectId(), filename, options);
    }

    @Override
    public GridFSUploadStream openUploadStream(final BsonValue id, final String filename) {
        return openUploadStream(id, filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final BsonValue id, final String filename, final GridFSUploadOptions options) {
        return createGridFSUploadStream(null, id, filename, options);
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final String filename) {
        return openUploadStream(clientSession, new BsonObjectId(), filename);
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final String filename,
                                               final GridFSUploadOptions options) {
        return openUploadStream(clientSession, new BsonObjectId(), filename, options);
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final BsonValue id, final String filename) {
        return openUploadStream(clientSession, id, filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final BsonValue id, final String filename,
                                               final GridFSUploadOptions options) {
        notNull("clientSession", clientSession);
        return createGridFSUploadStream(clientSession, id, filename, options);
    }

    private GridFSUploadStream createGridFSUploadStream(final ClientSession clientSession, final BsonValue id, final String filename,
                                                final GridFSUploadOptions options) {
        notNull("options", options);
        int chunkSize = options.getChunkSizeBytes() == null ? chunkSizeBytes : options.getChunkSizeBytes();
        return new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, id, filename, chunkSize, options.getMetadata(),
                new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection));
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
    public void uploadFromStream(final ClientSession clientSession, final String filename, final AsyncInputStream source,
                                 final SingleResultCallback<ObjectId> callback) {
        uploadFromStream(clientSession, filename, source, new GridFSUploadOptions(), callback);
    }

    @Override
    public void uploadFromStream(final ClientSession clientSession, final String filename, final AsyncInputStream source,
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
    public void uploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename,
                                 final AsyncInputStream source, final SingleResultCallback<Void> callback) {
        uploadFromStream(clientSession, id, filename, source, new GridFSUploadOptions(), callback);
    }

    @Override
    public void uploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename,
                                 final AsyncInputStream source, final GridFSUploadOptions options,
                                 final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeUploadFromStream(clientSession, id, filename, source, options, callback);
    }

    private void executeUploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename,
                                 final AsyncInputStream source, final GridFSUploadOptions options,
                                 final SingleResultCallback<Void> callback) {
        notNull("filename", filename);
        notNull("source", source);
        notNull("options", options);
        notNull("callback", callback);
        int chunkSize = options.getChunkSizeBytes() == null ? chunkSizeBytes : options.getChunkSizeBytes();
        GridFSUploadStream uploadStream;
        if (clientSession != null){
            uploadStream = openUploadStream(clientSession, id, filename, options);
        } else {
            uploadStream = openUploadStream(id, filename, options);
        }
        readAndWriteInputStream(source, uploadStream, ByteBuffer.allocate(chunkSize), errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ObjectId id) {
        notNull("id", id);
        return createGridFSDownloadStream(null, find(new Document("_id", id)));
    }
    @Override
    public GridFSDownloadStream openDownloadStream(final BsonValue id) {
        notNull("id", id);
        return createGridFSDownloadStream(null, find(new Document("_id", id)));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename) {
        return openDownloadStream(filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename, final GridFSDownloadOptions options) {
        return createGridFSDownloadStream(null, createGridFSFindIterable(null, filename, options));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final ObjectId id) {
        notNull("id", id);
        return createGridFSDownloadStream(clientSession, find(clientSession, new Document("_id", id)));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final BsonValue id) {
        notNull("id", id);
        return createGridFSDownloadStream(clientSession, find(clientSession, new Document("_id", id)));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final String filename) {
        return openDownloadStream(clientSession, filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final String filename,
                                                   final GridFSDownloadOptions options) {
        notNull("clientSession", clientSession);
        return createGridFSDownloadStream(clientSession, createGridFSFindIterable(clientSession, filename, options));
    }

    private GridFSDownloadStream createGridFSDownloadStream(final ClientSession clientSession,
                                                            final GridFSFindIterable gridFSFindIterable) {
        return new GridFSDownloadStreamImpl(clientSession, gridFSFindIterable, chunksCollection);
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
    public void downloadToStream(final ClientSession clientSession, final ObjectId id, final AsyncOutputStream destination,
                                 final SingleResultCallback<Long> callback) {
        downloadToAsyncOutputStream(openDownloadStream(clientSession, id), destination, errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final BsonValue id, final AsyncOutputStream destination,
                                 final SingleResultCallback<Long> callback) {
        downloadToAsyncOutputStream(openDownloadStream(clientSession, id), destination, errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final String filename, final AsyncOutputStream destination,
                                 final SingleResultCallback<Long> callback) {
        downloadToStream(clientSession, filename, destination, new GridFSDownloadOptions(), callback);
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final String filename, final AsyncOutputStream destination,
                                 final GridFSDownloadOptions options, final SingleResultCallback<Long> callback) {
        notNull("clientSession", clientSession);
        downloadToAsyncOutputStream(openDownloadStream(clientSession, filename, options), destination,
                errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public GridFSFindIterable find() {
        return createGridFSFindIterable(null, null);
    }

    @Override
    public GridFSFindIterable find(final Bson filter) {
        notNull("filter", filter);
        return createGridFSFindIterable(null, filter);
    }

    @Override
    public GridFSFindIterable find(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createGridFSFindIterable(clientSession, null);
    }

    @Override
    public GridFSFindIterable find(final ClientSession clientSession, final Bson filter) {
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
    public void delete(final ClientSession clientSession, final ObjectId id, final SingleResultCallback<Void> callback) {
        delete(clientSession, new BsonObjectId(id), callback);
    }

    @Override
    public void delete(final ClientSession clientSession, final BsonValue id, final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeDelete(clientSession, id, callback);
    }

   private void executeDelete(final ClientSession clientSession, final BsonValue id, final SingleResultCallback<Void> callback) {
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
    public void rename(final ClientSession clientSession, final ObjectId id, final String newFilename,
                       final SingleResultCallback<Void> callback) {
        rename(clientSession, new BsonObjectId(id), newFilename, callback);
    }

    @Override
    public void rename(final ClientSession clientSession, final BsonValue id, final String newFilename,
                       final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeRename(clientSession, id, newFilename, callback);
    }

    private void executeRename(final ClientSession clientSession, final BsonValue id, final String newFilename,
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
    public void drop(final ClientSession clientSession, final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeDrop(clientSession, callback);
    }

    private void executeDrop(final ClientSession clientSession, final SingleResultCallback<Void> callback) {
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

    private GridFSFindIterable createGridFSFindIterable(final ClientSession clientSession, final Bson filter) {
        return new GridFSFindIterableImpl(createFindIterable(clientSession, filter));
    }

    private GridFSFindIterable createGridFSFindIterable(final ClientSession clientSession, final String filename,
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

    private FindIterable<GridFSFile> createFindIterable(final ClientSession clientSession, final Bson filter) {
        FindIterable<GridFSFile> findIterable;
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

    private static MongoCollection<GridFSFile> getFilesCollection(final MongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".files", GridFSFile.class).withCodecRegistry(
                fromRegistries(database.getCodecRegistry(), MongoClients.getDefaultCodecRegistry())
        );
    }

    private static MongoCollection<Document> getChunksCollection(final MongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".chunks").withCodecRegistry(MongoClients.getDefaultCodecRegistry());
    }

    private void downloadToAsyncOutputStream(final GridFSDownloadStream downloadStream, final AsyncOutputStream destination,
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
                                         final GridFSUploadStream uploadStream, final ByteBuffer buffer,
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

    private void readAndWriteOutputStream(final AsyncOutputStream destination, final GridFSDownloadStream downloadStream,
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
