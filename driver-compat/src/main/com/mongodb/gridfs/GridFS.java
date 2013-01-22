/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of GridFS v1.0
 * <p/>
 * <a href="http://www.mongodb.org/display/DOCS/GridFS+Specification">GridFS 1.0 spec</a>
 *
 * @dochub gridfs
 */
@SuppressWarnings("rawtypes")
public class GridFS {

    /**
     * file's chunk size
     */
    public static final int DEFAULT_CHUNKSIZE = 256 * 1024;

    /**
     * file's max chunk size
     */
    public static final long MAX_CHUNKSIZE = (long) (3.5 * 1000 * 1000);

    /**
     * bucket to use for the collection namespaces
     */
    public static final String DEFAULT_BUCKET = "fs";

    // --------------------------
    // ------ constructors -------
    // --------------------------

    /**
     * Creates a GridFS instance for the default bucket "fs" in the given database. Set the preferred WriteConcern on
     * the give DB with DB.setWriteConcern
     *
     * @param db database to work with
     * @throws com.mongodb.MongoException
     * @see com.mongodb.WriteConcern
     */
    public GridFS(final DB db) {
        this(db, DEFAULT_BUCKET);
    }

    /**
     * Creates a GridFS instance for the specified bucket in the given database.  Set the preferred WriteConcern on the
     * give DB with DB.setWriteConcern
     *
     * @param db     database to work with
     * @param bucket bucket to use in the given database
     * @throws com.mongodb.MongoException
     * @see com.mongodb.WriteConcern
     */
    public GridFS(final DB db, final String bucket) {
        _db = db;
        _bucketName = bucket;

        _filesCollection = _db.getCollection(_bucketName + ".files");
        _chunkCollection = _db.getCollection(_bucketName + ".chunks");

        // ensure standard indexes as long as collections are small
        if (_filesCollection.count() < 1000) {
            _filesCollection.ensureIndex(new BasicDBObject("filename", 1).append("uploadDate", 1));
        }
        if (_chunkCollection.count() < 1000) {
            _chunkCollection.ensureIndex(new BasicDBObject("files_id", 1).append("n", 1),
                                        new BasicDBObject("unique", 1));
        }

        _filesCollection.setObjectClass(GridFSDBFile.class);
    }


    // --------------------------
    // ------ utils       -------
    // --------------------------


    /**
     * gets the list of files stored in this gridfs, sorted by filename
     *
     * @return cursor of file objects
     */
    public DBCursor getFileList() {
        return _filesCollection.find().sort(new BasicDBObject("filename", 1));
    }

    /**
     * gets a filtered list of files stored in this gridfs, sorted by filename
     *
     * @param query filter to apply
     * @return cursor of file objects
     */
    public DBCursor getFileList(final DBObject query) {
        return _filesCollection.find(query).sort(new BasicDBObject("filename", 1));
    }


    // --------------------------
    // ------ reading     -------
    // --------------------------

    /**
     * finds one file matching the given id. Equivalent to findOne(id)
     *
     * @param id
     * @return
     * @throws com.mongodb.MongoException
     */
    public GridFSDBFile find(final ObjectId id) {
        return findOne(id);
    }

    /**
     * finds one file matching the given id.
     *
     * @param id
     * @return
     * @throws com.mongodb.MongoException
     */
    public GridFSDBFile findOne(final ObjectId id) {
        return findOne(new BasicDBObject("_id", id));
    }

    /**
     * finds one file matching the given filename
     *
     * @param filename
     * @return
     * @throws com.mongodb.MongoException
     */
    public GridFSDBFile findOne(final String filename) {
        return findOne(new BasicDBObject("filename", filename));
    }

    /**
     * finds one file matching the given query
     *
     * @param query
     * @return
     * @throws com.mongodb.MongoException
     */
    public GridFSDBFile findOne(final DBObject query) {
        return _fix(_filesCollection.findOne(query));
    }

    /**
     * finds a list of files matching the given filename
     *
     * @param filename
     * @return
     * @throws com.mongodb.MongoException
     */
    public List<GridFSDBFile> find(final String filename) {
        return find(new BasicDBObject("filename", filename));
    }

    /**
     * finds a list of files matching the given query
     *
     * @param query
     * @return
     * @throws com.mongodb.MongoException
     */
    public List<GridFSDBFile> find(final DBObject query) {
        final List<GridFSDBFile> files = new ArrayList<GridFSDBFile>();

        final DBCursor c = _filesCollection.find(query);
        while (c.hasNext()) {
            files.add(_fix(c.next()));
        }
        return files;
    }

    private GridFSDBFile _fix(final Object o) {
        if (o == null) {
            return null;
        }

        if (!(o instanceof GridFSDBFile)) {
            throw new RuntimeException("somehow didn't get a GridFSDBFile");
        }

        final GridFSDBFile f = (GridFSDBFile) o;
        f._fs = this;
        return f;
    }


    // --------------------------
    // ------ remove      -------
    // --------------------------

    /**
     * removes the file matching the given id
     *
     * @param id
     * @throws com.mongodb.MongoException
     */
    public void remove(final ObjectId id) {
        _filesCollection.remove(new BasicDBObject("_id", id));
        _chunkCollection.remove(new BasicDBObject("files_id", id));
    }

    /**
     * removes all files matching the given filename
     *
     * @param filename
     * @throws com.mongodb.MongoException
     */
    public void remove(final String filename) {
        remove(new BasicDBObject("filename", filename));
    }

    /**
     * removes all files matching the given query
     *
     * @param query
     * @throws com.mongodb.MongoException
     */
    public void remove(final DBObject query) {
        for (final GridFSDBFile f : find(query)) {
            f.remove();
        }
    }


    // --------------------------
    // ------ writing     -------
    // --------------------------

    /**
     * creates a file entry. After calling this method, you have to call save() on the GridFSInputFile file
     *
     * @param data the file's data
     * @return
     */
    public GridFSInputFile createFile(final byte[] data) {
        return createFile(new ByteArrayInputStream(data), true);
    }


    /**
     * creates a file entry. After calling this method, you have to call save() on the GridFSInputFile file
     *
     * @param f the file object
     * @return
     * @throws IOException
     */
    public GridFSInputFile createFile(final File f) throws IOException {
        return createFile(new FileInputStream(f), f.getName(), true);
    }

    /**
     * creates a file entry. after calling this method, you have to call save() on the GridFSInputFile file
     *
     * @param in an inputstream containing the file's data
     * @return
     */
    public GridFSInputFile createFile(final InputStream in) {
        return createFile(in, null);
    }

    /**
     * creates a file entry. after calling this method, you have to call save() on the GridFSInputFile file
     *
     * @param in                   an inputstream containing the file's data
     * @param closeStreamOnPersist indicate the passed in input stream should be closed once the data chunk persisted
     * @return
     */
    public GridFSInputFile createFile(final InputStream in, final boolean closeStreamOnPersist) {
        return createFile(in, null, closeStreamOnPersist);
    }

    /**
     * creates a file entry. After calling this method, you have to call save() on the GridFSInputFile file
     *
     * @param in       an inputstream containing the file's data
     * @param filename the file name as stored in the db
     * @return
     */
    public GridFSInputFile createFile(final InputStream in, final String filename) {
        return new GridFSInputFile(this, in, filename);
    }

    /**
     * creates a file entry. After calling this method, you have to call save() on the GridFSInputFile file
     *
     * @param in                   an inputstream containing the file's data
     * @param filename             the file name as stored in the db
     * @param closeStreamOnPersist indicate the passed in input stream should be closed once the data chunk persisted
     * @return
     */
    public GridFSInputFile createFile(final InputStream in, final String filename, final boolean closeStreamOnPersist) {
        return new GridFSInputFile(this, in, filename, closeStreamOnPersist);
    }

    /**
     * @param filename the file name as stored in the db
     * @return
     * @see {@link GridFS#createFile()} on how to use this method
     */
    public GridFSInputFile createFile(final String filename) {
        return new GridFSInputFile(this, filename);
    }

    /**
     * This method creates an empty {@link GridFSInputFile} instance. On this instance an {@link java.io.OutputStream}
     * can be obtained using the {@link GridFSInputFile#getOutputStream()} method. You can still call {@link
     * GridFSInputFile#setContentType(String)} and {@link GridFSInputFile#setFilename(String)}. The file will be
     * completely written and closed after calling the {@link java.io.OutputStream#close()} method on the output
     * stream.
     *
     * @return GridFS file handle instance.
     */
    public GridFSInputFile createFile() {
        return new GridFSInputFile(this);
    }


    // --------------------------
    // ------ members     -------
    // --------------------------

    /**
     * gets the bucket name used in the collection's namespace
     *
     * @return
     */
    public String getBucketName() {
        return _bucketName;
    }

    /**
     * gets the db used
     *
     * @return
     */
    public DB getDB() {
        return _db;
    }

    protected final DB _db;
    protected final String _bucketName;
    protected final DBCollection _filesCollection;
    protected final DBCollection _chunkCollection;

}
