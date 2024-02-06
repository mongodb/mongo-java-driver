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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoGridFSException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.internal.TimeoutHelper;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.TimeoutContext;

import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

final class GridFSBucketImpl implements GridFSBucket {
    private static final int DEFAULT_CHUNKSIZE_BYTES = 255 * 1024;
    private static final String TIMEOUT_MESSAGE = "GridFS operation timed out";
    private final String bucketName;
    private final int chunkSizeBytes;
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<Document> chunksCollection;
    private volatile boolean checkedIndexes;

    GridFSBucketImpl(final MongoDatabase database) {
        this(database, "fs");
    }

    GridFSBucketImpl(final MongoDatabase database, final String bucketName) {
        this(notNull("bucketName", bucketName), DEFAULT_CHUNKSIZE_BYTES,
                getFilesCollection(notNull("database", database), bucketName),
                getChunksCollection(database, bucketName));
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
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
    public Long getTimeout(final TimeUnit timeUnit) {
        return filesCollection.getTimeout(timeUnit);
    }

    @Override
    public GridFSBucket withChunkSizeBytes(final int chunkSizeBytes) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection, chunksCollection);
    }

    @Override
    public GridFSBucket withReadPreference(final ReadPreference readPreference) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withReadPreference(readPreference),
                chunksCollection.withReadPreference(readPreference));
    }

    @Override
    public GridFSBucket withWriteConcern(final WriteConcern writeConcern) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withWriteConcern(writeConcern),
                chunksCollection.withWriteConcern(writeConcern));
    }

    @Override
    public GridFSBucket withReadConcern(final ReadConcern readConcern) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withReadConcern(readConcern),
                chunksCollection.withReadConcern(readConcern));
    }

    @Override
    public GridFSBucket withTimeout(final long timeout, final TimeUnit timeUnit) {
        isTrueArgument("timeout >= 0", timeout >= 0);
        notNull("timeUnit", timeUnit);
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withTimeout(timeout, timeUnit),
                chunksCollection.withTimeout(timeout, timeUnit));
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
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final ObjectId id, final String filename) {
        return openUploadStream(clientSession, new BsonObjectId(id), filename);
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

    private GridFSUploadStream createGridFSUploadStream(@Nullable final ClientSession clientSession, final BsonValue id,
                                                        final String filename, final GridFSUploadOptions options) {
        Timeout operationTimeout = startTimeout();
        notNull("options", options);
        Integer chunkSizeBytes = options.getChunkSizeBytes();
        int chunkSize = chunkSizeBytes == null ? this.chunkSizeBytes : chunkSizeBytes;
        checkCreateIndex(clientSession, operationTimeout);
        return new GridFSUploadStreamImpl(clientSession, filesCollection,
                chunksCollection, id, filename, chunkSize,
                options.getMetadata(), operationTimeout);
    }

    @Override
    public ObjectId uploadFromStream(final String filename, final InputStream source) {
        return uploadFromStream(filename, source, new GridFSUploadOptions());
    }

    @Override
    public ObjectId uploadFromStream(final String filename, final InputStream source, final GridFSUploadOptions options) {
        ObjectId id = new ObjectId();
        uploadFromStream(new BsonObjectId(id), filename, source, options);
        return id;
    }

    @Override
    public void uploadFromStream(final BsonValue id, final String filename, final InputStream source) {
        uploadFromStream(id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public void uploadFromStream(final BsonValue id, final String filename, final InputStream source,
                                 final GridFSUploadOptions options) {
        executeUploadFromStream(null, id, filename, source, options);
    }

    @Override
    public ObjectId uploadFromStream(final ClientSession clientSession, final String filename, final InputStream source) {
        return uploadFromStream(clientSession, filename, source, new GridFSUploadOptions());
    }

    @Override
    public ObjectId uploadFromStream(final ClientSession clientSession, final String filename, final InputStream source,
                                     final GridFSUploadOptions options) {
        ObjectId id = new ObjectId();
        uploadFromStream(clientSession, new BsonObjectId(id), filename, source, options);
        return id;
    }

    @Override
    public void uploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename, final InputStream source) {
        uploadFromStream(clientSession, id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public void uploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename, final InputStream source,
                                 final GridFSUploadOptions options) {
        notNull("clientSession", clientSession);
        executeUploadFromStream(clientSession, id, filename, source, options);
    }

    private void executeUploadFromStream(@Nullable final ClientSession clientSession, final BsonValue id, final String filename,
                                         final InputStream source, final GridFSUploadOptions options) {
        GridFSUploadStream uploadStream = createGridFSUploadStream(clientSession, id, filename, options);
        Integer chunkSizeBytes = options.getChunkSizeBytes();
        int chunkSize = chunkSizeBytes == null ? this.chunkSizeBytes : chunkSizeBytes;
        byte[] buffer = new byte[chunkSize];
        int len;
        try {
            while ((len = source.read(buffer)) != -1) {
                uploadStream.write(buffer, 0, len);
            }
            uploadStream.close();
        } catch (IOException e) {
            uploadStream.abort();
            throw new MongoGridFSException("IOException when reading from the InputStream", e);
        }
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ObjectId id) {
        return openDownloadStream(new BsonObjectId(id));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final BsonValue id) {
        Timeout operationTimeout = startTimeout();

        GridFSFile fileInfo = getFileInfoById(null, id, operationTimeout);
        return createGridFSDownloadStream(null, fileInfo, operationTimeout);
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename) {
        return openDownloadStream(filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename, final GridFSDownloadOptions options) {
        Timeout operationTimeout = startTimeout();
        GridFSFile file = getFileByName(null, filename, options, operationTimeout);
        return createGridFSDownloadStream(null, file, operationTimeout);
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final ObjectId id) {
        return openDownloadStream(clientSession, new BsonObjectId(id));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final BsonValue id) {
        notNull("clientSession", clientSession);
        Timeout operationTimeout = startTimeout();
        GridFSFile fileInfoById = getFileInfoById(clientSession, id, operationTimeout);
        return createGridFSDownloadStream(clientSession, fileInfoById, operationTimeout);
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final String filename) {
        return openDownloadStream(clientSession, filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final String filename,
                                                   final GridFSDownloadOptions options) {
        notNull("clientSession", clientSession);
        Timeout operationTimeout = startTimeout();
        GridFSFile file = getFileByName(clientSession, filename, options, operationTimeout);
        return createGridFSDownloadStream(clientSession, file, operationTimeout);
    }

    private GridFSDownloadStream createGridFSDownloadStream(@Nullable final ClientSession clientSession, final GridFSFile gridFSFile,
                                                            @Nullable final Timeout operationTimeout) {
        return new GridFSDownloadStreamImpl(clientSession, gridFSFile, chunksCollection, operationTimeout);
    }

    @Override
    public void downloadToStream(final ObjectId id, final OutputStream destination) {
        downloadToStream(new BsonObjectId(id), destination);
    }

    @Override
    public void downloadToStream(final BsonValue id, final OutputStream destination) {
        downloadToStream(openDownloadStream(id), destination);
    }

    @Override
    public void downloadToStream(final String filename, final OutputStream destination) {
        downloadToStream(filename, destination, new GridFSDownloadOptions());
    }

    @Override
    public void downloadToStream(final String filename, final OutputStream destination, final GridFSDownloadOptions options) {
        downloadToStream(openDownloadStream(filename, options), destination);
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final ObjectId id, final OutputStream destination) {
        downloadToStream(clientSession, new BsonObjectId(id), destination);
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final BsonValue id, final OutputStream destination) {
        notNull("clientSession", clientSession);
        downloadToStream(openDownloadStream(clientSession, id), destination);
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final String filename, final OutputStream destination) {
        downloadToStream(clientSession, filename, destination, new GridFSDownloadOptions());
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final String filename, final OutputStream destination,
                                 final GridFSDownloadOptions options) {
        notNull("clientSession", clientSession);
        downloadToStream(openDownloadStream(clientSession, filename, options), destination);
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

    private GridFSFindIterable createGridFSFindIterable(@Nullable final ClientSession clientSession, @Nullable final Bson filter) {
        return new GridFSFindIterableImpl(createFindIterable(clientSession, filter, startTimeout()));
    }

    private GridFSFindIterable createGridFSFindIterable(@Nullable final ClientSession clientSession, @Nullable final Bson filter,
                                                        @Nullable final Timeout operationTimeout) {
        return new GridFSFindIterableImpl(createFindIterable(clientSession, filter, operationTimeout));
    }

    @Override
    public void delete(final ObjectId id) {
        delete(new BsonObjectId(id));
    }

    @Override
    public void delete(final BsonValue id) {
        executeDelete(null, id);
    }

    @Override
    public void delete(final ClientSession clientSession, final ObjectId id) {
        delete(clientSession, new BsonObjectId(id));
    }

    @Override
    public void delete(final ClientSession clientSession, final BsonValue id) {
        notNull("clientSession", clientSession);
        executeDelete(clientSession, id);
    }

    private void executeDelete(@Nullable final ClientSession clientSession, final BsonValue id) {
        Timeout operationTimeout = startTimeout();
        DeleteResult result;
        if (clientSession != null) {
            result = withNullableTimeout(filesCollection, operationTimeout)
                    .deleteOne(clientSession, new BsonDocument("_id", id));
            withNullableTimeout(chunksCollection, operationTimeout)
                    .deleteMany(clientSession, new BsonDocument("files_id", id));
        } else {
            result = withNullableTimeout(filesCollection, operationTimeout)
                    .deleteOne(new BsonDocument("_id", id));
            withNullableTimeout(chunksCollection, operationTimeout)
                    .deleteMany(new BsonDocument("files_id", id));
        }

        if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
            throw new MongoGridFSException(format("No file found with the id: %s", id));
        }
    }

    @Override
    public void rename(final ObjectId id, final String newFilename) {
        rename(new BsonObjectId(id), newFilename);
    }

    @Override
    public void rename(final BsonValue id, final String newFilename) {
        executeRename(null, id, newFilename);
    }

    @Override
    public void rename(final ClientSession clientSession, final ObjectId id, final String newFilename) {
        rename(clientSession, new BsonObjectId(id), newFilename);
    }

    @Override
    public void rename(final ClientSession clientSession, final BsonValue id, final String newFilename) {
        notNull("clientSession", clientSession);
        executeRename(clientSession, id, newFilename);
    }

    private void executeRename(@Nullable final ClientSession clientSession, final BsonValue id, final String newFilename) {
        Timeout operationTimeout = startTimeout();
        UpdateResult updateResult;
        if (clientSession != null) {
            updateResult = withNullableTimeout(filesCollection, operationTimeout).updateOne(clientSession, new BsonDocument("_id", id),
                    new BsonDocument("$set", new BsonDocument("filename", new BsonString(newFilename))));
        } else {
            updateResult = withNullableTimeout(filesCollection, operationTimeout).updateOne(new BsonDocument("_id", id),
                    new BsonDocument("$set", new BsonDocument("filename", new BsonString(newFilename))));
        }

        if (updateResult.wasAcknowledged() && updateResult.getMatchedCount() == 0) {
            throw new MongoGridFSException(format("No file found with the id: %s", id));
        }
    }

    @Override
    public void drop() {
        Timeout operationTimeout = startTimeout();
        withNullableTimeout(filesCollection, operationTimeout).drop();
        withNullableTimeout(chunksCollection, operationTimeout).drop();
    }

    @Override
    public void drop(final ClientSession clientSession) {
        Timeout operationTimeout = startTimeout();
        notNull("clientSession", clientSession);
        withNullableTimeout(filesCollection, operationTimeout).drop(clientSession);
        withNullableTimeout(chunksCollection, operationTimeout).drop(clientSession);
    }

    private static MongoCollection<GridFSFile> getFilesCollection(final MongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".files", GridFSFile.class).withCodecRegistry(
                fromRegistries(database.getCodecRegistry(), MongoClientSettings.getDefaultCodecRegistry())
        );
    }

    private static MongoCollection<Document> getChunksCollection(final MongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".chunks").withCodecRegistry(MongoClientSettings.getDefaultCodecRegistry());
    }

    private void checkCreateIndex(@Nullable final ClientSession clientSession, @Nullable final Timeout operationTimeout) {
        if (!checkedIndexes) {
            if (collectionIsEmpty(clientSession,
                    filesCollection.withDocumentClass(Document.class).withReadPreference(primary()),
                    operationTimeout)) {

                Document filesIndex = new Document("filename", 1).append("uploadDate", 1);
                if (!hasIndex(clientSession, filesCollection.withReadPreference(primary()), filesIndex, operationTimeout)) {
                    createIndex(clientSession, filesCollection, filesIndex, new IndexOptions(), operationTimeout);
                }
                Document chunksIndex = new Document("files_id", 1).append("n", 1);
                if (!hasIndex(clientSession, chunksCollection.withReadPreference(primary()), chunksIndex, operationTimeout)) {
                    createIndex(clientSession, chunksCollection, chunksIndex, new IndexOptions().unique(true), operationTimeout);
                }
            }
            checkedIndexes = true;
        }
    }

    private <T> boolean collectionIsEmpty(@Nullable final ClientSession clientSession,
                                          final MongoCollection<T> collection,
                                          @Nullable final Timeout operationTimeout) {
        if (clientSession != null) {
            return withNullableTimeout(collection, operationTimeout)
                    .find(clientSession).projection(new Document("_id", 1)).first() == null;
        } else {
            return withNullableTimeout(collection, operationTimeout)
                    .find().projection(new Document("_id", 1)).first() == null;
        }
    }

    private <T> boolean hasIndex(@Nullable final ClientSession clientSession, final MongoCollection<T> collection,
                                 final Document index, @Nullable final Timeout operationTimeout) {
        boolean hasIndex = false;
        ListIndexesIterable<Document> listIndexesIterable;
        if (clientSession != null) {
            listIndexesIterable = withNullableTimeout(collection, operationTimeout).listIndexes(clientSession);
        } else {
            listIndexesIterable = withNullableTimeout(collection, operationTimeout).listIndexes();
        }

        ArrayList<Document> indexes = listIndexesIterable.into(new ArrayList<>());
        for (Document result : indexes) {
            Document indexDoc = result.get("key", new Document());
            for (final Map.Entry<String, Object> entry : indexDoc.entrySet()) {
                if (entry.getValue() instanceof Number) {
                    entry.setValue(((Number) entry.getValue()).intValue());
                }
            }
            if (indexDoc.equals(index)) {
                hasIndex = true;
                break;
            }
        }
        return hasIndex;
    }

    private <T> void createIndex(@Nullable final ClientSession clientSession, final MongoCollection<T> collection, final Document index,
                                 final IndexOptions indexOptions, final @Nullable Timeout operationTimeout) {
        if (clientSession != null) {
            withNullableTimeout(collection, operationTimeout).createIndex(clientSession, index, indexOptions);
        } else {
            withNullableTimeout(collection, operationTimeout).createIndex(index, indexOptions);
        }
    }

    private GridFSFile getFileByName(@Nullable final ClientSession clientSession, final String filename,
                                     final GridFSDownloadOptions options, @Nullable final Timeout operationTimeout) {
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

        GridFSFile fileInfo = createGridFSFindIterable(clientSession, new Document("filename", filename), operationTimeout).skip(skip)
                .sort(new Document("uploadDate", sort)).first();
        if (fileInfo == null) {
            throw new MongoGridFSException(format("No file found with the filename: %s and revision: %s", filename, revision));
        }
        return fileInfo;
    }

    private GridFSFile getFileInfoById(@Nullable final ClientSession clientSession, final BsonValue id,
                                       @Nullable final Timeout operationTImeout) {
        notNull("id", id);
        GridFSFile fileInfo = createFindIterable(clientSession, new Document("_id", id), operationTImeout).first();
        if (fileInfo == null) {
            throw new MongoGridFSException(format("No file found with the id: %s", id));
        }
        return fileInfo;
    }

    private FindIterable<GridFSFile> createFindIterable(@Nullable final ClientSession clientSession, @Nullable final Bson filter,
                                                        @Nullable final Timeout operationTImeout) {
        FindIterable<GridFSFile> findIterable;
        if (clientSession != null) {
            findIterable = withNullableTimeout(filesCollection, operationTImeout).find(clientSession);
        } else {
            findIterable = withNullableTimeout(filesCollection, operationTImeout).find();
        }
        if (filter != null) {
            findIterable = findIterable.filter(filter);
        }
        if (filesCollection.getTimeout(MILLISECONDS) != null) {
            findIterable.timeoutMode(TimeoutMode.CURSOR_LIFETIME);
        }
        return findIterable;
    }

    private void downloadToStream(final GridFSDownloadStream downloadStream, final OutputStream destination) {
        byte[] buffer = new byte[downloadStream.getGridFSFile().getChunkSize()];
        int len;
        MongoGridFSException savedThrowable = null;
        try {
            while ((len = downloadStream.read(buffer)) != -1) {
                destination.write(buffer, 0, len);
            }
        } catch (MongoOperationTimeoutException e){ // TODO (CSOT) - JAVA-5248 Update to MongoOperationTimeoutException
            throw e;
        } catch (IOException e) {
            savedThrowable = new MongoGridFSException("IOException when reading from the OutputStream", e);
        } catch (Exception e) {
            savedThrowable = new MongoGridFSException("Unexpected Exception when reading GridFS and writing to the Stream", e);
        } finally {
            try {
                downloadStream.close();
            } catch (Exception e) {
                // Do nothing
            }
            if (savedThrowable != null) {
                throw savedThrowable;
            }
        }
    }

    private static <T> MongoCollection<T> withNullableTimeout(final MongoCollection<T> chunksCollection,
                                                              @Nullable final Timeout timeout) {
        return TimeoutHelper.collectionWithTimeout(chunksCollection, TIMEOUT_MESSAGE, timeout);
    }

    @Nullable
    private Timeout startTimeout() {
        return TimeoutContext.calculateTimeout(filesCollection.getTimeout(MILLISECONDS));
    }
}
