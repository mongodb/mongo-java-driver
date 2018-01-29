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

package com.mongodb.gridfs;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <p> Implementation of GridFS - a specification for storing and retrieving files that exceed the BSON-document size limit of 16MB. </p>
 *
 * <p>Instead of storing a file in a single document, GridFS divides a file into parts, or chunks, and stores each of those chunks as a
 * separate document. By default GridFS limits chunk size to 255k. GridFS uses two collections to store files. One collection stores the
 * file chunks, and the other stores file metadata. </p>
 *
 * <p>When you query a GridFS store for a file, the driver or client will reassemble the chunks as needed. You can perform range queries on
 * files stored through GridFS. You also can access information from arbitrary sections of files, which allows you to "skip" into the middle
 * of a video or audio file. </p>
 *
 * <p>GridFS is useful not only for storing files that exceed 16MB but also for storing any files for which you want access without having
 * to load the entire file into memory. For more information on the indications of GridFS, see MongoDB official documentation.</p>
 *
 * @mongodb.driver.manual core/gridfs/ GridFS
 */
@SuppressWarnings("rawtypes")
public class GridFS {

    /**
     * File's chunk size
     */
    public static final int DEFAULT_CHUNKSIZE = 255 * 1024;
    /**
     * File's max chunk size
     *
     * @deprecated You can calculate max chunkSize with a similar formula {@link com.mongodb.MongoClient#getMaxBsonObjectSize()} - 500*1000.
     * Please ensure that you left enough space for metadata (500kb is enough).
     */
    @Deprecated
    public static final long MAX_CHUNKSIZE = (long) (3.5 * 1000 * 1000);
    /**
     * Bucket to use for the collection namespaces
     */
    public static final String DEFAULT_BUCKET = "fs";

    private final DB database;
    private final String bucketName;

    private final DBCollection filesCollection;
    private final DBCollection chunksCollection;

    /**
     * Creates a GridFS instance for the default bucket "fs" in the given database. Set the preferred WriteConcern on the give DB with
     * DB.setWriteConcern
     *
     * @param db database to work with
     * @throws com.mongodb.MongoException if there's a failure
     * @see com.mongodb.WriteConcern
     */
    public GridFS(final DB db) {
        this(db, DEFAULT_BUCKET);
    }

    /**
     * Creates a GridFS instance for the specified bucket in the given database.  Set the preferred WriteConcern on the give DB with
     * DB.setWriteConcern
     *
     * @param db     database to work with
     * @param bucket bucket to use in the given database
     * @throws com.mongodb.MongoException if there's a failure
     * @see com.mongodb.WriteConcern
     */
    public GridFS(final DB db, final String bucket) {
        this.database = db;
        this.bucketName = bucket;

        this.filesCollection = database.getCollection(bucketName + ".files");
        this.chunksCollection = database.getCollection(bucketName + ".chunks");

        // ensure standard indexes as long as collections are small
        try {
            if (filesCollection.count() < 1000) {
                filesCollection.createIndex(new BasicDBObject("filename", 1).append("uploadDate", 1));
            }
            if (chunksCollection.count() < 1000) {
                chunksCollection.createIndex(new BasicDBObject("files_id", 1).append("n", 1),
                                             new BasicDBObject("unique", true));
            }
        } catch (MongoException e) {
            //TODO: Logging
        }

        filesCollection.setObjectClass(GridFSDBFile.class);
    }

    /**
     * Gets the list of files stored in this gridfs, sorted by filename.
     *
     * @return cursor of file objects
     */
    public DBCursor getFileList() {
        return filesCollection.find().sort(new BasicDBObject("filename", 1));
    }

    /**
     * Gets a filtered list of files stored in this gridfs, sorted by filename.
     *
     * @param query filter to apply
     * @return cursor of file objects
     */
    public DBCursor getFileList(final DBObject query) {
        return filesCollection.find(query).sort(new BasicDBObject("filename", 1));
    }

    /**
     * Gets a sorted, filtered list of files stored in this gridfs.
     *
     * @param query filter to apply
     * @param sort  sorting to apply
     * @return cursor of file objects
     */
    public DBCursor getFileList(final DBObject query, final DBObject sort) {
        return filesCollection.find(query).sort(sort);
    }

    /**
     * Finds one file matching the given objectId. Equivalent to findOne(objectId).
     *
     * @param objectId the objectId of the file stored on a server
     * @return a gridfs file
     * @throws com.mongodb.MongoException if the operation fails
     */
    public GridFSDBFile find(final ObjectId objectId) {
        return findOne(objectId);
    }

    /**
     * Finds one file matching the given objectId.
     *
     * @param objectId the objectId of the file stored on a server
     * @return a gridfs file
     * @throws com.mongodb.MongoException if the operation fails
     */
    public GridFSDBFile findOne(final ObjectId objectId) {
        return findOne(new BasicDBObject("_id", objectId));
    }

    /**
     * Finds one file matching the given filename.
     *
     * @param filename the name of the file stored on a server
     * @return the gridfs db file
     * @throws com.mongodb.MongoException if the operation fails
     */
    public GridFSDBFile findOne(final String filename) {
        return findOne(new BasicDBObject("filename", filename));
    }

    /**
     * Finds one file matching the given query.
     *
     * @param query filter to apply
     * @return a gridfs file
     * @throws com.mongodb.MongoException if the operation fails
     */
    public GridFSDBFile findOne(final DBObject query) {
        return injectGridFSInstance(filesCollection.findOne(query));
    }

    /**
     * Finds a list of files matching the given filename.
     *
     * @param filename the filename to look for
     * @return list of gridfs files
     * @throws com.mongodb.MongoException if the operation fails
     */
    public List<GridFSDBFile> find(final String filename) {
        return find(new BasicDBObject("filename", filename));
    }

    /**
     * Finds a list of files matching the given filename.
     *
     * @param filename the filename to look for
     * @param sort     the fields to sort with
     * @return list of gridfs files
     * @throws com.mongodb.MongoException if the operation fails
     */
    public List<GridFSDBFile> find(final String filename, final DBObject sort) {
        return find(new BasicDBObject("filename", filename), sort);
    }

    /**
     * Finds a list of files matching the given query.
     *
     * @param query the filter to apply
     * @return list of gridfs files
     * @throws com.mongodb.MongoException if the operation fails
     */
    public List<GridFSDBFile> find(final DBObject query) {
        return find(query, null);
    }

    /**
     * Finds a list of files matching the given query.
     *
     * @param query the filter to apply
     * @param sort  the fields to sort with
     * @return list of gridfs files
     * @throws com.mongodb.MongoException if the operation fails
     */
    public List<GridFSDBFile> find(final DBObject query, final DBObject sort) {
        List<GridFSDBFile> files = new ArrayList<GridFSDBFile>();

        DBCursor cursor = filesCollection.find(query);
        if (sort != null) {
            cursor.sort(sort);
        }

        try {
            while (cursor.hasNext()) {
                files.add(injectGridFSInstance(cursor.next()));
            }
        } finally {
            cursor.close();
        }
        return Collections.unmodifiableList(files);
    }

    private GridFSDBFile injectGridFSInstance(final Object o) {
        if (o == null) {
            return null;
        }

        if (!(o instanceof GridFSDBFile)) {
            throw new IllegalArgumentException("somehow didn't get a GridFSDBFile");
        }

        GridFSDBFile f = (GridFSDBFile) o;
        f.fs = this;
        return f;
    }

    /**
     * Removes the file matching the given id.
     *
     * @param id the id of the file to be removed
     * @throws com.mongodb.MongoException if the operation fails
     */
    public void remove(final ObjectId id) {
        if (id == null) {
            throw new IllegalArgumentException("file id can not be null");
        }

        filesCollection.remove(new BasicDBObject("_id", id));
        chunksCollection.remove(new BasicDBObject("files_id", id));
    }

    /**
     * Removes all files matching the given filename.
     *
     * @param filename the name of the file to be removed
     * @throws com.mongodb.MongoException if the operation fails
     */
    public void remove(final String filename) {
        if (filename == null) {
            throw new IllegalArgumentException("filename can not be null");
        }

        remove(new BasicDBObject("filename", filename));
    }

    /**
     * Removes all files matching the given query.
     *
     * @param query filter to apply
     * @throws com.mongodb.MongoException if the operation fails
     */
    public void remove(final DBObject query) {
        if (query == null) {
            throw new IllegalArgumentException("query can not be null");
        }

        for (final GridFSDBFile f : find(query)) {
            f.remove();
        }
    }

    /**
     * Creates a file entry. After calling this method, you have to call {@link com.mongodb.gridfs.GridFSInputFile#save()}.
     *
     * @param data the file's data
     * @return a gridfs input file
     */
    public GridFSInputFile createFile(final byte[] data) {
        return createFile(new ByteArrayInputStream(data), true);
    }

    /**
     * Creates a file entry. After calling this method, you have to call {@link com.mongodb.gridfs.GridFSInputFile#save()}.
     *
     * @param file the file object
     * @return a GridFS input file
     * @throws IOException if there are problems reading {@code file}
     */
    public GridFSInputFile createFile(final File file) throws IOException {
        return createFile(new FileInputStream(file), file.getName(), true);
    }

    /**
     * Creates a file entry. After calling this method, you have to call {@link com.mongodb.gridfs.GridFSInputFile#save()}.
     *
     * @param in an inputstream containing the file's data
     * @return a gridfs input file
     */
    public GridFSInputFile createFile(final InputStream in) {
        return createFile(in, null);
    }

    /**
     * Creates a file entry. After calling this method, you have to call {@link com.mongodb.gridfs.GridFSInputFile#save()}.
     *
     * @param in                   an inputstream containing the file's data
     * @param closeStreamOnPersist indicate the passed in input stream should be closed once the data chunk persisted
     * @return a gridfs input file
     */
    public GridFSInputFile createFile(final InputStream in, final boolean closeStreamOnPersist) {
        return createFile(in, null, closeStreamOnPersist);
    }

    /**
     * Creates a file entry. After calling this method, you have to call {@link com.mongodb.gridfs.GridFSInputFile#save()}.
     *
     * @param in       an inputstream containing the file's data
     * @param filename the file name as stored in the db
     * @return a gridfs input file
     */
    public GridFSInputFile createFile(final InputStream in, final String filename) {
        return new GridFSInputFile(this, in, filename);
    }

    /**
     * Creates a file entry. After calling this method, you have to call {@link com.mongodb.gridfs.GridFSInputFile#save()}.
     *
     * @param in                   an inputstream containing the file's data
     * @param filename             the file name as stored in the db
     * @param closeStreamOnPersist indicate the passed in input stream should be closed once the data chunk persisted
     * @return a gridfs input file
     */
    public GridFSInputFile createFile(final InputStream in, final String filename, final boolean closeStreamOnPersist) {
        return new GridFSInputFile(this, in, filename, closeStreamOnPersist);
    }

    /**
     * Creates a file entry.
     *
     * @param filename the file name as stored in the db
     * @return a gridfs input file
     * @see GridFS#createFile()
     */
    public GridFSInputFile createFile(final String filename) {
        return new GridFSInputFile(this, filename);
    }

    /**
     * This method creates an empty {@link GridFSInputFile} instance. On this instance an {@link java.io.OutputStream} can be obtained using
     * the {@link GridFSInputFile#getOutputStream()} method. You can still call {@link GridFSInputFile#setContentType(String)} and {@link
     * GridFSInputFile#setFilename(String)}. The file will be completely written and closed after calling the {@link
     * java.io.OutputStream#close()} method on the output stream.
     *
     * @return GridFS file handle instance.
     */
    public GridFSInputFile createFile() {
        return new GridFSInputFile(this);
    }

    /**
     * Gets the bucket name used in the collection's namespace. Default value is 'fs'.
     *
     * @return the name of the file bucket
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * Gets the database used.
     *
     * @return the database
     */
    public DB getDB() {
        return database;
    }

    /**
     * Gets the {@link DBCollection} in which the file's metadata is stored.
     *
     * @return the collection
     */
    protected DBCollection getFilesCollection() {
        return filesCollection;
    }

    /**
     * Gets the {@link DBCollection} in which the binary chunks are stored.
     *
     * @return the collection
     */
    protected DBCollection getChunksCollection() {
        return chunksCollection;
    }

}
