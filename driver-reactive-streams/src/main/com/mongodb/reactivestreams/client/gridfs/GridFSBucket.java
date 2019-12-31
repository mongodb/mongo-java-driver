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

package com.mongodb.reactivestreams.client.gridfs;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;

/**
 * Represents a GridFS Bucket
 *
 * @since 1.3
 */
@ThreadSafe
public interface GridFSBucket {

    /**
     * The bucket name.
     *
     * @return the bucket name
     */
    String getBucketName();

    /**
     * Sets the chunk size in bytes. Defaults to 255.
     *
     * @return the chunk size in bytes.
     */
    int getChunkSizeBytes();

    /**
     * Get the write concern for the GridFSBucket.
     *
     * @return the {@link com.mongodb.WriteConcern}
     */
    WriteConcern getWriteConcern();

    /**
     * Get the read preference for the GridFSBucket.
     *
     * @return the {@link com.mongodb.ReadPreference}
     */
    ReadPreference getReadPreference();

    /**
     * Get the read concern for the GridFSBucket.
     *
     * @return the {@link com.mongodb.ReadConcern}
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    ReadConcern getReadConcern();

    /**
     * Create a new GridFSBucket instance with a new chunk size in bytes.
     *
     * @param chunkSizeBytes the new chunk size in bytes.
     * @return a new GridFSBucket instance with the different chunk size in bytes
     */
    GridFSBucket withChunkSizeBytes(int chunkSizeBytes);

    /**
     * Create a new GridFSBucket instance with a different read preference.
     *
     * @param readPreference the new {@link ReadPreference} for the database
     * @return a new GridFSBucket instance with the different readPreference
     */
    GridFSBucket withReadPreference(ReadPreference readPreference);

    /**
     * Create a new GridFSBucket instance with a different write concern.
     *
     * @param writeConcern the new {@link WriteConcern} for the database
     * @return a new GridFSBucket instance with the different writeConcern
     */
    GridFSBucket withWriteConcern(WriteConcern writeConcern);

    /**
     * Create a new MongoDatabase instance with a different read concern.
     *
     * @param readConcern the new {@link ReadConcern} for the database
     * @return a new GridFSBucket instance with the different ReadConcern
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    GridFSBucket withReadConcern(ReadConcern readConcern);

    /**
     * Uploads the contents of the given {@code Publisher} to a GridFS bucket.
     * <p>
     * Reads the contents of the user file from the {@code source} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     * </p>
     *
     * @param filename the filename
     * @param source   the Publisher providing the file data
     * @return a Publisher with a single element, the ObjectId of the uploaded file.
     * @since 1.13
     */
    GridFSUploadPublisher<ObjectId> uploadFromPublisher(String filename, Publisher<ByteBuffer> source);

    /**
     * Uploads the contents of the given {@code Publisher} to a GridFS bucket.
     * <p>
     * Reads the contents of the user file from the {@code source} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     * </p>
     *
     * @param filename the filename
     * @param source   the Publisher providing the file data
     * @param options  the GridFSUploadOptions
     * @return a Publisher with a single element, the ObjectId of the uploaded file.
     * @since 1.13
     */
    GridFSUploadPublisher<ObjectId> uploadFromPublisher(String filename, Publisher<ByteBuffer> source, GridFSUploadOptions options);

    /**
     * Uploads the contents of the given {@code Publisher} to a GridFS bucket.
     * <p>
     * Reads the contents of the user file from the {@code source} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     * </p>
     *
     * @param id the custom id value of the file
     * @param filename the filename
     * @param source   the Publisher providing the file data
     * @return a Publisher with a single element, representing when the successful upload of the source.
     * @since 1.13
     */
    GridFSUploadPublisher<Void> uploadFromPublisher(BsonValue id, String filename, Publisher<ByteBuffer> source);

    /**
     * Uploads the contents of the given {@code Publisher} to a GridFS bucket.
     * <p>
     * Reads the contents of the user file from the {@code source} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     * </p>
     *
     * @param id       the custom id value of the file
     * @param filename the filename
     * @param source   the Publisher providing the file data
     * @param options  the GridFSUploadOptions
     * @return a Publisher with a single element, representing when the successful upload of the source.
     * @since 1.13
     */
    GridFSUploadPublisher<Void> uploadFromPublisher(BsonValue id, String filename, Publisher<ByteBuffer> source,
                                                    GridFSUploadOptions options);

    /**
     * Uploads the contents of the given {@code Publisher} to a GridFS bucket.
     * <p>
     * Reads the contents of the user file from the {@code source} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     * </p>
     *
     * @param clientSession the client session with which to associate this operation
     * @param filename the filename
     * @param source   the Publisher providing the file data
     * @return a Publisher with a single element, the ObjectId of the uploaded file.
     * @mongodb.server.release 3.6
     * @since 1.13
     */
    GridFSUploadPublisher<ObjectId> uploadFromPublisher(ClientSession clientSession, String filename, Publisher<ByteBuffer> source);

    /**
     * Uploads the contents of the given {@code Publisher} to a GridFS bucket.
     * <p>
     * Reads the contents of the user file from the {@code source} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     * </p>
     *
     * @param clientSession the client session with which to associate this operation
     * @param filename the filename
     * @param source   the Publisher providing the file data
     * @param options  the GridFSUploadOptions
     * @return a Publisher with a single element, the ObjectId of the uploaded file.
     * @mongodb.server.release 3.6
     * @since 1.13
     */
    GridFSUploadPublisher<ObjectId> uploadFromPublisher(ClientSession clientSession, String filename, Publisher<ByteBuffer> source,
                                                        GridFSUploadOptions options);

    /**
     * Uploads the contents of the given {@code Publisher} to a GridFS bucket.
     * <p>
     * Reads the contents of the user file from the {@code source} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     * </p>
     *
     * @param clientSession the client session with which to associate this operation
     * @param id the custom id value of the file
     * @param filename the filename
     * @param source   the Publisher providing the file data
     * @return a Publisher with a single element, representing when the successful upload of the source.
     * @mongodb.server.release 3.6
     * @since 1.13
     */
    GridFSUploadPublisher<Void> uploadFromPublisher(ClientSession clientSession, BsonValue id, String filename,
                                                    Publisher<ByteBuffer> source);

    /**
     * Uploads the contents of the given {@code Publisher} to a GridFS bucket.
     * <p>
     * Reads the contents of the user file from the {@code source} and uploads it as chunks in the chunks collection. After all the
     * chunks have been uploaded, it creates a files collection document for {@code filename} in the files collection.
     * </p>
     *
     * @param clientSession the client session with which to associate this operation
     * @param id       the custom id value of the file
     * @param filename the filename
     * @param source   the Publisher providing the file data
     * @param options  the GridFSUploadOptions
     * @return a Publisher with a single element, representing when the successful upload of the source.
     * @mongodb.server.release 3.6
     * @since 1.13
     */
    GridFSUploadPublisher<Void> uploadFromPublisher(ClientSession clientSession, BsonValue id, String filename,
                                                    Publisher<ByteBuffer> source, GridFSUploadOptions options);

    /**
     * Downloads the contents of the stored file specified by {@code id} into the {@code Publisher}.
     *
     * @param id          the ObjectId of the file to be written to the destination Publisher
     * @return a Publisher with a single element, representing the amount of data written
     * @since 1.13
     */
    GridFSDownloadPublisher downloadToPublisher(ObjectId id);

    /**
     * Downloads the contents of the stored file specified by {@code id} into the {@code Publisher}.
     *
     * @param id          the custom id of the file, to be written to the destination Publisher
     * @return a Publisher with a single element, representing the amount of data written
     * @since 1.13
     */
    GridFSDownloadPublisher downloadToPublisher(BsonValue id);

    /**
     * Downloads the contents of the stored file specified by {@code filename} into the {@code Publisher}.
     *
     * @param filename    the name of the file to be downloaded
     * @return a Publisher with a single element, representing the amount of data written
     * @since 1.13
     */
    GridFSDownloadPublisher downloadToPublisher(String filename);

    /**
     * Downloads the contents of the stored file specified by {@code filename} and by the revision in {@code options} into the
     * {@code Publisher}.
     *
     * @param filename    the name of the file to be downloaded
     * @param options     the download options
     * @return a Publisher with a single element, representing the amount of data written
     * @since 1.13
     */
    GridFSDownloadPublisher downloadToPublisher(String filename, GridFSDownloadOptions options);

    /**
     * Downloads the contents of the stored file specified by {@code id} into the {@code Publisher}.
     *
     * @param clientSession the client session with which to associate this operation
     * @param id          the ObjectId of the file to be written to the destination Publisher
     * @return a Publisher with a single element, representing the amount of data written
     * @mongodb.server.release 3.6
     * @since 1.13
     */
    GridFSDownloadPublisher downloadToPublisher(ClientSession clientSession, ObjectId id);

    /**
     * Downloads the contents of the stored file specified by {@code id} into the {@code Publisher}.
     *
     * @param clientSession the client session with which to associate this operation
     * @param id          the custom id of the file, to be written to the destination Publisher
     * @return a Publisher with a single element, representing the amount of data written
     * @mongodb.server.release 3.6
     * @since 1.13
     */
    GridFSDownloadPublisher downloadToPublisher(ClientSession clientSession, BsonValue id);

    /**
     * Downloads the contents of the latest version of the stored file specified by {@code filename} into the {@code Publisher}.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filename    the name of the file to be downloaded
     * @return a Publisher with a single element, representing the amount of data written
     * @mongodb.server.release 3.6
     * @since 1.13
     */
    GridFSDownloadPublisher downloadToPublisher(ClientSession clientSession, String filename);

    /**
     * Downloads the contents of the stored file specified by {@code filename} and by the revision in {@code options} into the
     * {@code Publisher}.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filename    the name of the file to be downloaded
     * @param options     the download options
     * @return a Publisher with a single element, representing the amount of data written
     * @mongodb.server.release 3.6
     * @since 1.13
     */
    GridFSDownloadPublisher downloadToPublisher(ClientSession clientSession, String filename, GridFSDownloadOptions options);

    /**
     * Finds all documents in the files collection.
     *
     * @return the GridFS find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    GridFSFindPublisher find();

    /**
     * Finds all documents in the collection that match the filter.
     * <p>
     * Below is an example of filtering against the filename and some nested metadata that can also be stored along with the file data:
     * <pre>
     *  {@code
     *      Filters.and(Filters.eq("filename", "mongodb.png"), Filters.eq("metadata.contentType", "image/png"));
     *  }
     *  </pre>
     *
     * @param filter the query filter
     * @return the GridFS find iterable interface
     * @see com.mongodb.client.model.Filters
     */
    GridFSFindPublisher find(Bson filter);

    /**
     * Finds all documents in the files collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the GridFS find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    GridFSFindPublisher find(ClientSession clientSession);

    /**
     * Finds all documents in the collection that match the filter.
     * <p>
     * Below is an example of filtering against the filename and some nested metadata that can also be stored along with the file data:
     * <pre>
     *  {@code
     *      Filters.and(Filters.eq("filename", "mongodb.png"), Filters.eq("metadata.contentType", "image/png"));
     *  }
     *  </pre>
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @return the GridFS find iterable interface
     * @see com.mongodb.client.model.Filters
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    GridFSFindPublisher find(ClientSession clientSession, Bson filter);

    /**
     * Given a {@code id}, delete this stored file's files collection document and associated chunks from a GridFS bucket.
     *
     * @param id       the ObjectId of the file to be deleted
     * @return a publisher with a single element, representing that the file has been deleted
     */
    Publisher<Void> delete(ObjectId id);

    /**
     * Given a {@code id}, delete this stored file's files collection document and associated chunks from a GridFS bucket.
     *
     * @param id       the ObjectId of the file to be deleted
     * @return a publisher with a single element, representing that the file has been deleted
     */
    Publisher<Void> delete(BsonValue id);

    /**
     * Given a {@code id}, delete this stored file's files collection document and associated chunks from a GridFS bucket.
     *
     * @param clientSession the client session with which to associate this operation
     * @param id       the ObjectId of the file to be deleted
     * @return a publisher with a single element, representing that the file has been deleted
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> delete(ClientSession clientSession, ObjectId id);

    /**
     * Given a {@code id}, delete this stored file's files collection document and associated chunks from a GridFS bucket.
     *
     * @param clientSession the client session with which to associate this operation
     * @param id       the ObjectId of the file to be deleted
     * @return a publisher with a single element, representing that the file has been deleted
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> delete(ClientSession clientSession, BsonValue id);

    /**
     * Renames the stored file with the specified {@code id}.
     *
     * @param id          the id of the file in the files collection to rename
     * @param newFilename the new filename for the file
     * @return a publisher with a single element, representing that the file has been renamed
     */
    Publisher<Void> rename(ObjectId id, String newFilename);

    /**
     * Renames the stored file with the specified {@code id}.
     *
     * @param id          the id of the file in the files collection to rename
     * @param newFilename the new filename for the file
     * @return a publisher with a single element, representing that the file has been renamed
     */
    Publisher<Void> rename(BsonValue id, String newFilename);

    /**
     * Renames the stored file with the specified {@code id}.
     *
     * @param clientSession the client session with which to associate this operation
     * @param id          the id of the file in the files collection to rename
     * @param newFilename the new filename for the file
     * @return a publisher with a single element, representing that the file has been renamed
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> rename(ClientSession clientSession, ObjectId id, String newFilename);

    /**
     * Renames the stored file with the specified {@code id}.
     *
     * @param clientSession the client session with which to associate this operation
     * @param id          the id of the file in the files collection to rename
     * @param newFilename the new filename for the file
     * @return a publisher with a single element, representing that the file has been renamed
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> rename(ClientSession clientSession, BsonValue id, String newFilename);

    /**
     * Drops the data associated with this bucket from the database.
     *
     * @return a publisher with a single element, representing that the collections have been dropped
     */
    Publisher<Void> drop();

    /**
     * Drops the data associated with this bucket from the database.
     *
     * @param clientSession the client session with which to associate this operation
     * @return a publisher with a single element, representing that the collections have been dropped
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> drop(ClientSession clientSession);
}
