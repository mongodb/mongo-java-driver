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

/**
 *  Implementation of GridFS v1.0
 *
 *  <a href="http://www.mongodb.org/display/DOCS/GridFS+Specification">GridFS 1.0 spec</a>
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
     * @param mongo database to work with
     */
    public GridFS(Mongo mongo) {
        this(mongo, DEFAULT_BUCKET);
    }

    /**
     * Creates a GridFS instance for the specified bucket
     * in the given database.
     *
     * @param mongo database to work with
     * @param bucket bucket to use in the given database
     */
    public GridFS(Mongo mongo, String bucket) {
        _mongo = mongo;
        _bucketName = bucket;

        _filesCollection = _mongo.getCollection( _bucketName + ".files" );
        _chunkCollection = _mongo.getCollection( _bucketName + ".chunks" );

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
     */
    public GridFSInputFile createFile( InputStream in , String filename ){
        return new GridFSInputFile( this , in , filename );
    }




    // --------------------------
    // ------ members     -------
    // --------------------------


    protected final Mongo _mongo;
    protected final String _bucketName;
    protected final DBCollection _filesCollection;
    protected final DBCollection _chunkCollection;

}
