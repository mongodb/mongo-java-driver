// GridFS.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb.gridfs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.mongodb.MongoException;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 *  Implementation of GridFS v1.0
 *
 *  <a href="http://www.mongodb.org/display/DOCS/GridFS+Specification">GridFS 1.0 spec</a>
 *
 * @dochub gridfs
 */
public class GridFS {
    private static final Logger LOGGER = Logger.getLogger( "com.mongodb.gridfs" );

    /**
     * file's chunk size
     */
    public static final int DEFAULT_CHUNKSIZE = 256 * 1024;

    /**
     * file's max chunk size
     *
     * @deprecated You can calculate max chunkSize with
     *             a similar formula {@link com.mongodb.MongoClient#getMaxBsonObjectSize()} - 500*1000.
     *             Please ensure that you left enough space for metadata (500kb is enough).
     */
    @Deprecated
    public static final long MAX_CHUNKSIZE = (long) (3.5 * 1000 * 1000);

    /**
     * bucket to use for the collection namespaces
     */
    public static final String DEFAULT_BUCKET = "fs";

    // --------------------------
    // ------ constructors -------
    // --------------------------

    /**
     * Creates a GridFS instance for the default bucket "fs"
     * in the given database. Set the preferred WriteConcern on the give DB with DB.setWriteConcern
     * @see com.mongodb.WriteConcern
     * @param db database to work with
     * @throws MongoException 
     */
    public GridFS(DB db) {
        this(db, DEFAULT_BUCKET);
    }

    /**
     * Creates a GridFS instance for the specified bucket
     * in the given database.  Set the preferred WriteConcern on the give DB with DB.setWriteConcern
     *
     * @see com.mongodb.WriteConcern
     * @param db database to work with
     * @param bucket bucket to use in the given database
     * @throws MongoException 
     */
    public GridFS(DB db, String bucket) {
        _db = db;
        _bucketName = bucket;

        _filesCollection = _db.getCollection( _bucketName + ".files" );
        _chunkCollection = _db.getCollection( _bucketName + ".chunks" );

        // ensure standard indexes as long as collections are small
        try {
            if (_filesCollection.count() < 1000) {
                _filesCollection.ensureIndex( BasicDBObjectBuilder.start().add( "filename" , 1 ).add( "uploadDate" , 1 ).get() );
            }
            if (_chunkCollection.count() < 1000) {
                _chunkCollection.ensureIndex( BasicDBObjectBuilder.start().add( "files_id" , 1 ).add( "n" , 1 ).get() ,
                        BasicDBObjectBuilder.start().add( "unique" , true ).get() );
            }
        } catch (MongoException e) {
             LOGGER.info(String.format("Unable to ensure indices on GridFS collections in database %s", db.getName()));
        }
        _filesCollection.setObjectClass( GridFSDBFile.class );
    }


    // --------------------------
    // ------ utils       -------
    // --------------------------


    /**
     * gets the list of files stored in this gridfs, sorted by filename
     *
     * @return cursor of file objects
     */
    public DBCursor getFileList(){
        return getFileList(new BasicDBObject());
    }

    /**
     * gets a filtered list of files stored in this gridfs, sorted by filename
     *
     * @param query filter to apply
     * @return cursor of file objects
     */
    public DBCursor getFileList( DBObject query ){
        return getFileList(query, new BasicDBObject("filename",1));
    }

    /**
     * gets a filtered list of files stored in this gridfs, sorted by param sort
     *
     * @param query filter to apply
     * @param sort sorting to apply
     * @return cursor of file objects
     */
    public DBCursor getFileList( DBObject query, DBObject sort){
        return _filesCollection.find( query ).sort(sort);
    }


    // --------------------------
    // ------ reading     -------
    // --------------------------

    /**
     * finds one file matching the given id. Equivalent to findOne(id)
     * @param id
     * @return
     * @throws MongoException 
     */
    public GridFSDBFile find( ObjectId id ){
        return findOne( id );
    }
    /**
     * finds one file matching the given id.
     * @param id
     * @return
     * @throws MongoException 
     */
    public GridFSDBFile findOne( ObjectId id ){
        return findOne( new BasicDBObject( "_id" , id ) );
    }
    /**
     * finds one file matching the given filename
     * @param filename
     * @return
     * @throws MongoException 
     */
    public GridFSDBFile findOne( String filename ){
        return findOne( new BasicDBObject( "filename" , filename ) );
    }
    /**
     * finds one file matching the given query
     * @param query
     * @return
     * @throws MongoException 
     */
    public GridFSDBFile findOne( DBObject query ){
        return _fix( _filesCollection.findOne( query ) );
    }

    /**
     * finds a list of files matching the given filename
     * @param filename
     * @return
     * @throws MongoException 
     */
    public List<GridFSDBFile> find( String filename ){
        return find( filename, null );
    }

    /**
     * finds a list of files matching the given filename
     * @param filename
     * @param sort
     * @return
     * @throws MongoException
     */
    public List<GridFSDBFile> find( String filename , DBObject sort){
        return find( new BasicDBObject( "filename" , filename ), sort );
    }

    /**
     * finds a list of files matching the given query
     * @param query
     * @return
     * @throws MongoException
     */
    public List<GridFSDBFile> find( DBObject query ){
        return find(query, null);
    }

    /**
     * finds a list of files matching the given query
     * @param query
     * @param sort
     * @return
     * @throws MongoException 
     */
    public List<GridFSDBFile> find( DBObject query , DBObject sort){
        List<GridFSDBFile> files = new ArrayList<GridFSDBFile>();

        DBCursor c = null;
        try {
            c = _filesCollection.find( query );
            if (sort != null) {
                c.sort(sort);
            }
            while ( c.hasNext() ){
                files.add( _fix( c.next() ) );
            }
        } finally {
             if (c != null){
                 c.close();
             }
        }
        return files;
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    protected GridFSDBFile _fix( Object o ){
        if ( o == null )
            return null;

        if ( ! ( o instanceof GridFSDBFile ) )
            throw new RuntimeException( "somehow didn't get a GridFSDBFile" );

        GridFSDBFile f = (GridFSDBFile)o;
        f._fs = this;
        return f;
    }


    // --------------------------
    // ------ remove      -------
    // --------------------------

    /**
     * removes the file matching the given id
     * @param id
     * @throws MongoException 
     */
    public void remove( ObjectId id ){
        _filesCollection.remove( new BasicDBObject( "_id" , id ) );
        _chunkCollection.remove( new BasicDBObject( "files_id" , id ) );
    }

    /**
     * removes all files matching the given filename
     * @param filename
     * @throws MongoException 
     */
    public void remove( String filename ){
        remove( new BasicDBObject( "filename" , filename ) );
    }

    /**
     * removes all files matching the given query
     * @param query
     * @throws MongoException 
     */
    public void remove( DBObject query ){
        for ( GridFSDBFile f : find( query ) ){
            f.remove();
        }
    }


    // --------------------------
    // ------ writing     -------
    // --------------------------

    /**
     * creates a file entry.
     * After calling this method, you have to call save() on the GridFSInputFile file
     * @param data the file's data
     * @return
     */
    public GridFSInputFile createFile( byte[] data ){
        return createFile( new ByteArrayInputStream( data ), true );
    }


    /**
     * creates a file entry.
     * After calling this method, you have to call save() on the GridFSInputFile file
     * @param f the file object
     * @return
     * @throws IOException
     */
    public GridFSInputFile createFile( File f )
        throws IOException {
        return createFile( new FileInputStream( f ) , f.getName(), true );
    }

    /**
     * creates a file entry.
     * after calling this method, you have to call save() on the GridFSInputFile file
     * @param in an inputstream containing the file's data
     * @return
     */
    public GridFSInputFile createFile( InputStream in ){
        return createFile( in , null );
    }

    /**
     * creates a file entry.
     * after calling this method, you have to call save() on the GridFSInputFile file
     * @param in an inputstream containing the file's data
     * @param closeStreamOnPersist indicate the passed in input stream should be closed
     *        once the data chunk persisted
     * @return
     */
    public GridFSInputFile createFile( InputStream in, boolean closeStreamOnPersist ){
        return createFile( in , null, closeStreamOnPersist );
    }

    /**
     * creates a file entry.
     * After calling this method, you have to call save() on the GridFSInputFile file
     * @param in an inputstream containing the file's data
     * @param filename the file name as stored in the db
     * @return
     */
    public GridFSInputFile createFile( InputStream in , String filename ){
        return new GridFSInputFile( this , in , filename );
    }

    /**
     * creates a file entry.
     * After calling this method, you have to call save() on the GridFSInputFile file
     * @param in an inputstream containing the file's data
     * @param filename the file name as stored in the db
     * @param closeStreamOnPersist indicate the passed in input stream should be closed
     *        once the data chunk persisted
     * @return
     */
    public GridFSInputFile createFile( InputStream in , String filename, boolean closeStreamOnPersist ){
        return new GridFSInputFile( this , in , filename, closeStreamOnPersist );
    }

    /**
     * @see {@link GridFS#createFile()} on how to use this method
     * @param filename the file name as stored in the db
     * @return
     */
    public GridFSInputFile createFile(String filename) {
        return new GridFSInputFile( this , filename );
    }

    /**
     * This method creates an empty {@link GridFSInputFile} instance. On this
     * instance an {@link java.io.OutputStream} can be obtained using the
     * {@link GridFSInputFile#getOutputStream()} method. You can still call
     * {@link GridFSInputFile#setContentType(String)} and
     * {@link GridFSInputFile#setFilename(String)}. The file will be completely
     * written and closed after calling the {@link java.io.OutputStream#close()}
     * method on the output stream.
     *
     * @return GridFS file handle instance.
     */
    public GridFSInputFile createFile() {
        return new GridFSInputFile( this );
    }



    // --------------------------
    // ------ members     -------
    // --------------------------

    /**
     * gets the bucket name used in the collection's namespace
     * @return
     */
    public String getBucketName(){
        return _bucketName;
    }

    /**
     * gets the db used
     * @return
     */
    public DB getDB(){
        return _db;
    }

    /**
     * Gets the {@link DBCollection} in which the file’s metadata is stored.
     *
     * @return the collection
     */
    protected DBCollection getFilesCollection() {
        return _filesCollection;
    }

    /**
     * Gets the {@link DBCollection} in which the binary chunks are stored.
     *
     * @return the collection
     */
    protected DBCollection getChunksCollection() {
        return _chunkCollection;
    }


    /**
     * @deprecated Please use {@link #getDB()} for access.
     */
    @Deprecated
    protected final DB _db;

    /**
     * @deprecated Please use {@link #getBucketName()} for access.
     */
    @Deprecated
    protected final String _bucketName;

    /**
     * @deprecated Please use {@link #getFilesCollection()} for access.
     */
    @Deprecated
    protected final DBCollection _filesCollection;

    /**
     * @deprecated Please use {@link #getChunksCollection()} for access.
     */
    @Deprecated
    protected final DBCollection _chunkCollection;

}
