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

import java.io.*;
import java.util.*;

import com.mongodb.*;

import org.bson.*;
import org.bson.types.*;

/**
 *  Implementation of GridFS v1.0
 *
 *  <a href="http://www.mongodb.org/display/DOCS/GridFS+Specification">GridFS 1.0 spec</a>
 * 
 * @dochub gridfs
 */
public class GridFS {

    public static final int DEFAULT_CHUNKSIZE = 256 * 1024;
    public static final String DEFAULT_BUCKET = "fs";

    // --------------------------
    // ------ constructors -------
    // --------------------------

    /**
     * Creates a GridFS instance for the default bucket "fs"
     * in the given database.
     *
     * @param db database to work with
     */
    public GridFS(DB db) {
        this(db, DEFAULT_BUCKET);
    }

    /**
     * Creates a GridFS instance for the specified bucket
     * in the given database.
     *
     * @param db database to work with
     * @param bucket bucket to use in the given database
     */
    public GridFS(DB db, String bucket) {
        _db = db;
        _bucketName = bucket;

        _filesCollection = _db.getCollection( _bucketName + ".files" );
        _chunkCollection = _db.getCollection( _bucketName + ".chunks" );

        _chunkCollection.ensureIndex( BasicDBObjectBuilder.start().add( "files_id" , 1 ).add( "n" , 1 ).get() );

        _filesCollection.setObjectClass( GridFSDBFile.class );
    }


    // --------------------------
    // ------ utils       -------
    // --------------------------


    /**
     *   Returns a cursor for this filestore
     *
     * @return cursor of file objects
     */
    public DBCursor getFileList(){
        return _filesCollection.find().sort(new BasicDBObject("filename",1));
    }

    /**
     *   Returns a cursor for this filestore
     *
     * @param query filter to apply
     * @return cursor of file objects
     */
    public DBCursor getFileList( DBObject query ){
        return _filesCollection.find( query ).sort(new BasicDBObject("filename",1));
    }


    // --------------------------
    // ------ reading     -------
    // --------------------------

    public GridFSDBFile find( ObjectId id ){
        return findOne( id );
    }
    public GridFSDBFile findOne( ObjectId id ){
        return findOne( new BasicDBObject( "_id" , id ) );
    }
    public GridFSDBFile findOne( String filename ){
        return findOne( new BasicDBObject( "filename" , filename ) );
    }
    public GridFSDBFile findOne( DBObject query ){
        return _fix( _filesCollection.findOne( query ) );
    }

    public List<GridFSDBFile> find( String filename ){
        return find( new BasicDBObject( "filename" , filename ) );
    }
    public List<GridFSDBFile> find( DBObject query ){
        List<GridFSDBFile> files = new ArrayList<GridFSDBFile>();

        DBCursor c = _filesCollection.find( query );
        while ( c.hasNext() ){
            files.add( _fix( c.next() ) );
        }
        return files;
    }

    private GridFSDBFile _fix( Object o ){
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

    public void remove( ObjectId id ){
        _filesCollection.remove( new BasicDBObject( "_id" , id ) );
        _chunkCollection.remove( new BasicDBObject( "files_id" , id ) );
    }
    
    public void remove( String filename ){
        remove( new BasicDBObject( "filename" , filename ) );
    }

    public void remove( DBObject query ){
        for ( GridFSDBFile f : find( query ) ){
            f.remove();
        }
    }
    

    // --------------------------
    // ------ writing     -------
    // --------------------------

    /**
     * after calling this method, you have to call save() on the GridFSInputFile file
     */
    public GridFSInputFile createFile( byte[] data ){
        return createFile( new ByteArrayInputStream( data ) );
    }


    /**
     * after calling this method, you have to call save() on the GridFSInputFile file
     */
    public GridFSInputFile createFile( File f )
        throws IOException {
        return createFile( new FileInputStream( f ) , f.getName() );
    }

    /**
     * after calling this method, you have to call save() on the GridFSInputFile file
     */
    public GridFSInputFile createFile( InputStream in ){
        return createFile( in , null );
    }

    /**
     * after calling this method, you have to call save() on the GridFSInputFile file
     * on that, you can call setFilename, setContentType and control meta data by modifying the 
     *   result of getMetaData
     */
    public GridFSInputFile createFile( InputStream in , String filename ){
        return new GridFSInputFile( this , in , filename );
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
     * @param filename
     *            Name of the file to be created.
     * @return GridFS file handle instance.
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

    public String getBucketName(){
        return _bucketName;
    }

    public DB getDB(){
        return _db;
    }

    protected final DB _db;
    protected final String _bucketName;
    protected final DBCollection _filesCollection;
    protected final DBCollection _chunkCollection;

}
