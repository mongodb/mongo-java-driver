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
import com.mongodb.MongoGridFSException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;
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

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

final class GridFSBucketImpl implements GridFSBucket {
    private static final int DEFAULT_CHUNKSIZE_BYTES = 255 * 1024;
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
        notNull("options", options);
        Integer chunkSizeBytes = options.getChunkSizeBytes();
        int chunkSize = chunkSizeBytes == null ? this.chunkSizeBytes : chunkSizeBytes;
        checkCreateIndex(clientSession);
        return new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, id, filename, chunkSize, options.getMetadata());
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
        return createGridFSDownloadStream(null, getFileInfoById(null, id));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename) {
        return openDownloadStream(filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename, final GridFSDownloadOptions options) {
        return createGridFSDownloadStream(null, getFileByName(null, filename, options));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final ObjectId id) {
        return openDownloadStream(clientSession, new BsonObjectId(id));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final BsonValue id) {
        notNull("clientSession", clientSession);
        return createGridFSDownloadStream(clientSession, getFileInfoById(clientSession, id));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final String filename) {
        return openDownloadStream(clientSession, filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final String filename,
                                                   final GridFSDownloadOptions options) {
        notNull("clientSession", clientSession);
        return createGridFSDownloadStream(clientSession, getFileByName(clientSession, filename, options));
    }

    private GridFSDownloadStream createGridFSDownloadStream(@Nullable final ClientSession clientSession, final GridFSFile gridFSFile) {
        return new GridFSDownloadStreamImpl(clientSession, gridFSFile, chunksCollection);
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
        return new GridFSFindIterableImpl(createFindIterable(clientSession, filter));
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
        DeleteResult result;
        if (clientSession != null) {
            result = filesCollection.deleteOne(clientSession, new BsonDocument("_id", id));
            chunksCollection.deleteMany(clientSession, new BsonDocument("files_id", id));
        } else {
            result = filesCollection.deleteOne(new BsonDocument("_id", id));
            chunksCollection.deleteMany(new BsonDocument("files_id", id));
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
        UpdateResult updateResult;
        if (clientSession != null) {
            updateResult = filesCollection.updateOne(clientSession, new BsonDocument("_id", id),
                    new BsonDocument("$set", new BsonDocument("filename", new BsonString(newFilename))));
        } else {
            updateResult = filesCollection.updateOne(new BsonDocument("_id", id),
                    new BsonDocument("$set", new BsonDocument("filename", new BsonString(newFilename))));
        }

        if (updateResult.wasAcknowledged() && updateResult.getMatchedCount() == 0) {
            throw new MongoGridFSException(format("No file found with the id: %s", id));
        }
    }

    @Override
    public void drop() {
        filesCollection.drop();
        chunksCollection.drop();
    }

    @Override
    public void drop(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        filesCollection.drop(clientSession);
        chunksCollection.drop(clientSession);
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public GridFSDownloadStream openDownloadStreamByName(final String filename) {
        return openDownloadStreamByName(filename, new com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions());
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public GridFSDownloadStream openDownloadStreamByName(final String filename,
                                                         final com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions options) {
        return openDownloadStream(filename, new GridFSDownloadOptions().revision(options.getRevision()));
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public void downloadToStreamByName(final String filename, final OutputStream destination) {
        downloadToStreamByName(filename, destination, new com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions());
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public void downloadToStreamByName(final String filename, final OutputStream destination,
                                       final com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions options) {
        downloadToStream(filename, destination, new GridFSDownloadOptions().revision(options.getRevision()));
    }

    private static MongoCollection<GridFSFile> getFilesCollection(final MongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".files", GridFSFile.class).withCodecRegistry(
                fromRegistries(database.getCodecRegistry(), MongoClientSettings.getDefaultCodecRegistry())
        );
    }

    private static MongoCollection<Document> getChunksCollection(final MongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".chunks").withCodecRegistry(MongoClientSettings.getDefaultCodecRegistry());
    }

    private void checkCreateIndex(@Nullable final ClientSession clientSession) {
        if (!checkedIndexes) {
            if (collectionIsEmpty(clientSession, filesCollection.withDocumentClass(Document.class).withReadPreference(primary()))) {
                Document filesIndex = new Document("filename", 1).append("uploadDate", 1);
                if (!hasIndex(clientSession, filesCollection.withReadPreference(primary()), filesIndex)) {
                    createIndex(clientSession, filesCollection, filesIndex, new IndexOptions());
                }
                Document chunksIndex = new Document("files_id", 1).append("n", 1);
                if (!hasIndex(clientSession, chunksCollection.withReadPreference(primary()), chunksIndex)) {
                    createIndex(clientSession, chunksCollection, chunksIndex, new IndexOptions().unique(true));
                }
            }
            checkedIndexes = true;
        }
    }

    private <T> boolean collectionIsEmpty(@Nullable final ClientSession clientSession, final MongoCollection<T> collection) {
        if (clientSession != null) {
            return collection.find(clientSession).projection(new Document("_id", 1)).first() == null;
        } else {
            return collection.find().projection(new Document("_id", 1)).first() == null;
        }
    }

    private <T> boolean hasIndex(@Nullable final ClientSession clientSession, final MongoCollection<T> collection, final Document index) {
        boolean hasIndex = false;
        ListIndexesIterable<Document> listIndexesIterable;
        if (clientSession != null) {
            listIndexesIterable = collection.listIndexes(clientSession);
        } else {
            listIndexesIterable = collection.listIndexes();
        }

        ArrayList<Document> indexes = listIndexesIterable.into(new ArrayList<Document>());
        for (Document indexDoc : indexes) {
            if (indexDoc.get("key", Document.class).equals(index)) {
                hasIndex = true;
                break;
            }
        }
        return hasIndex;
    }

    private <T> void createIndex(@Nullable final ClientSession clientSession, final MongoCollection<T> collection, final Document index,
                                 final IndexOptions indexOptions) {
       if (clientSession != null) {
           collection.createIndex(clientSession, index, indexOptions);
       } else {
           collection.createIndex(index, indexOptions);
       }
    }

    private GridFSFile getFileByName(@Nullable final ClientSession clientSession, final String filename,
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

        GridFSFile fileInfo = createGridFSFindIterable(clientSession, new Document("filename", filename)).skip(skip)
                .sort(new Document("uploadDate", sort)).first();
        if (fileInfo == null) {
            throw new MongoGridFSException(format("No file found with the filename: %s and revision: %s", filename, revision));
        }
        return fileInfo;
    }

    private GridFSFile getFileInfoById(@Nullable final ClientSession clientSession, final BsonValue id) {
        notNull("id", id);
        GridFSFile fileInfo = createFindIterable(clientSession, new Document("_id", id)).first();
        if (fileInfo == null) {
            throw new MongoGridFSException(format("No file found with the id: %s", id));
        }
        return fileInfo;
    }

    private FindIterable<GridFSFile> createFindIterable(@Nullable final ClientSession clientSession, @Nullable final Bson filter) {
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

    private void downloadToStream(final GridFSDownloadStream downloadStream, final OutputStream destination) {
        byte[] buffer = new byte[downloadStream.getGridFSFile().getChunkSize()];
        int len;
        MongoGridFSException savedThrowable = null;
        try {
            while ((len = downloadStream.read(buffer)) != -1) {
                destination.write(buffer, 0, len);
            }
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
}
