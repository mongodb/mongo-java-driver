/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal.gridfs;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.reactivestreams.client.internal.gridfs.GridFSPublisherCreator.createDeletePublisher;
import static com.mongodb.reactivestreams.client.internal.gridfs.GridFSPublisherCreator.createDropPublisher;
import static com.mongodb.reactivestreams.client.internal.gridfs.GridFSPublisherCreator.createGridFSDownloadPublisher;
import static com.mongodb.reactivestreams.client.internal.gridfs.GridFSPublisherCreator.createGridFSFindPublisher;
import static com.mongodb.reactivestreams.client.internal.gridfs.GridFSPublisherCreator.createGridFSUploadPublisher;
import static com.mongodb.reactivestreams.client.internal.gridfs.GridFSPublisherCreator.createRenamePublisher;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


/**
 * The internal GridFSBucket implementation.
 *
 * <p>This should not be considered a part of the public API.</p>
 */
public final class GridFSBucketImpl implements GridFSBucket {
    private static final int DEFAULT_CHUNKSIZE_BYTES = 255 * 1024;
    private final String bucketName;
    private final int chunkSizeBytes;
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<Document> chunksCollection;

    public GridFSBucketImpl(final MongoDatabase database) {
        this(database, "fs");
    }

    public GridFSBucketImpl(final MongoDatabase database, final String bucketName) {
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

    private static MongoCollection<GridFSFile> getFilesCollection(final MongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".files", GridFSFile.class).withCodecRegistry(
                fromRegistries(database.getCodecRegistry(), MongoClients.getDefaultCodecRegistry())
        );
    }

    private static MongoCollection<Document> getChunksCollection(final MongoDatabase database, final String bucketName) {
        return database.getCollection(bucketName + ".chunks").withCodecRegistry(MongoClients.getDefaultCodecRegistry());
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

    @Nullable
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
    public GridFSBucket withTimeout(final long timeout, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withTimeout(timeout, timeUnit),
                chunksCollection.withTimeout(timeout, timeUnit));
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final String filename, final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final String filename,
                                                               final Publisher<ByteBuffer> source,
                                                               final GridFSUploadOptions options) {
        return createGridFSUploadPublisher(chunkSizeBytes, filesCollection, chunksCollection,
                                           null, new BsonObjectId(), filename, options, source).withObjectId();
    }

    @Override
    public GridFSUploadPublisher<Void> uploadFromPublisher(final BsonValue id, final String filename, final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<Void> uploadFromPublisher(final BsonValue id, final String filename, final Publisher<ByteBuffer> source,
                                                           final GridFSUploadOptions options) {
        return createGridFSUploadPublisher(chunkSizeBytes, filesCollection, chunksCollection, null, id,
                                           filename, options, source);
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final ClientSession clientSession, final String filename,
                                                               final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(clientSession, filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final ClientSession clientSession, final String filename,
                                                               final Publisher<ByteBuffer> source, final GridFSUploadOptions options) {
        return createGridFSUploadPublisher(chunkSizeBytes, filesCollection, chunksCollection,
                                           notNull("clientSession", clientSession), new BsonObjectId(), filename, options, source)
                .withObjectId();
    }

    @Override
    public GridFSUploadPublisher<Void> uploadFromPublisher(final ClientSession clientSession, final BsonValue id,
                                                           final String filename, final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(clientSession, id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<Void> uploadFromPublisher(final ClientSession clientSession, final BsonValue id,
                                                           final String filename,
                                                           final Publisher<ByteBuffer> source,
                                                           final GridFSUploadOptions options) {
        return createGridFSUploadPublisher(chunkSizeBytes, filesCollection, chunksCollection,
                                           notNull("clientSession", clientSession), new BsonObjectId(), filename, options, source);
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ObjectId id) {
        return downloadToPublisher(new BsonObjectId(id));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final BsonValue id) {
        return createGridFSDownloadPublisher(chunksCollection, null,
                                             createGridFSFindPublisher(filesCollection, null, new BsonDocument("_id", id)));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final String filename) {
        return downloadToPublisher(filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final String filename, final GridFSDownloadOptions options) {
        return createGridFSDownloadPublisher(chunksCollection, null,
                                             createGridFSFindPublisher(filesCollection, null, filename, options));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final ObjectId id) {
        return downloadToPublisher(clientSession, new BsonObjectId(id));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final BsonValue id) {
        return createGridFSDownloadPublisher(chunksCollection, notNull("clientSession", clientSession),
                                             createGridFSFindPublisher(filesCollection, clientSession, new BsonDocument("_id", id)));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final String filename) {
        return downloadToPublisher(clientSession, filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession,
                                                       final String filename,
                                                       final GridFSDownloadOptions options) {
        return createGridFSDownloadPublisher(chunksCollection, notNull("clientSession", clientSession),
                                             createGridFSFindPublisher(filesCollection, clientSession, filename, options));
    }

    @Override
    public GridFSFindPublisher find() {
        return createGridFSFindPublisher(filesCollection, null, null);
    }

    @Override
    public GridFSFindPublisher find(final Bson filter) {
        return createGridFSFindPublisher(filesCollection, null, notNull("filter", filter));
    }

    @Override
    public GridFSFindPublisher find(final ClientSession clientSession) {
        return createGridFSFindPublisher(filesCollection, notNull("clientSession", clientSession), null);
    }

    @Override
    public GridFSFindPublisher find(final ClientSession clientSession, final Bson filter) {
        return createGridFSFindPublisher(filesCollection, notNull("clientSession", clientSession), notNull("filter", filter));
    }

    @Override
    public Publisher<Void> delete(final ObjectId id) {
        return delete(new BsonObjectId(id));
    }

    @Override
    public Publisher<Void> delete(final BsonValue id) {
        return createDeletePublisher(filesCollection, chunksCollection, null, id);
    }

    @Override
    public Publisher<Void> delete(final ClientSession clientSession, final ObjectId id) {
        return delete(clientSession, new BsonObjectId(id));
    }

    @Override
    public Publisher<Void> delete(final ClientSession clientSession, final BsonValue id) {
        return createDeletePublisher(filesCollection, chunksCollection, notNull("clientSession", clientSession), id);
    }

    @Override
    public Publisher<Void> rename(final ObjectId id, final String newFilename) {
        return rename(new BsonObjectId(id), newFilename);
    }

    @Override
    public Publisher<Void> rename(final BsonValue id, final String newFilename) {
        return createRenamePublisher(filesCollection, null, id, newFilename);
    }

    @Override
    public Publisher<Void> rename(final ClientSession clientSession, final ObjectId id, final String newFilename) {
        return rename(clientSession, new BsonObjectId(id), newFilename);
    }

    @Override
    public Publisher<Void> rename(final ClientSession clientSession, final BsonValue id, final String newFilename) {
        return createRenamePublisher(filesCollection, notNull("clientSession", clientSession), id, newFilename);
    }

    @Override
    public Publisher<Void> drop() {
        return createDropPublisher(filesCollection, chunksCollection, null);
    }

    @Override
    public Publisher<Void> drop(final ClientSession clientSession) {
        return createDropPublisher(filesCollection, chunksCollection, notNull("clientSession", clientSession));
    }
}
