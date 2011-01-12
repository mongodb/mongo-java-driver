// GridFSDBFile.java

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

import com.mongodb.*;
import com.mongodb.util.*;

import java.io.*;
import java.util.*;

/**
 * This class enables to retrieve a GridFS file metadata and content.
 * Operations include:
 * - writing data to a file on disk or an OutputStream
 * - getting each chunk as a byte array
 * - getting an InputStream to stream the data into
 * @author antoine
 */
public class GridFSDBFile extends GridFSFile {
    
    
    /**
     * Returns an InputStream from which data can be read
     * @return
     */
    public InputStream getInputStream(){
        return new MyInputStream();
    }

    /**
     * Writes the file's data to a file on disk
     * @param filename the file name on disk
     * @return
     * @throws IOException
     */
    public long writeTo( String filename ) throws IOException {
        return writeTo( new File( filename ) );
    }
    /**
     * Writes the file's data to a file on disk
     * @param f the File object
     * @return
     * @throws IOException
     */
    public long writeTo( File f ) throws IOException {
        return writeTo( new FileOutputStream( f ) );
    }

    /**
     * Writes the file's data to an OutputStream
     * @param out the OutputStream
     * @return
     * @throws IOException
     */
    public long writeTo( OutputStream out )
        throws IOException {
        final int nc = numChunks();
        for ( int i=0; i<nc; i++ ){
            out.write( getChunk( i ) );
        }
        return _length;
    }
    
    byte[] getChunk( int i ){
        if ( _fs == null )
            throw new RuntimeException( "no gridfs!" );
        
        DBObject chunk = _fs._chunkCollection.findOne( BasicDBObjectBuilder.start( "files_id" , _id )
                                                       .add( "n" , i ).get() );
        if ( chunk == null )
            throw new MongoException( "can't find a chunk!  file id: " + _id + " chunk: " + i );

        return (byte[])chunk.get( "data" );
    }

    class MyInputStream extends InputStream {

        MyInputStream(){
            _numChunks = numChunks();
        }
        
        public int available(){
            if ( _data == null )
                return 0;
            return _data.length - _offset;
        }
        
        public void close(){
        }

        public void mark(int readlimit){
            throw new RuntimeException( "mark not supported" );
        }
        public void reset(){
            throw new RuntimeException( "mark not supported" );
        }
        public boolean markSupported(){
            return false;
        }

        public int read(){
            byte b[] = new byte[1];
            int res = read( b );
            if ( res < 0 )
                return -1;
            return b[0] & 0xFF;
        }
        
        public int read(byte[] b){
            return read( b , 0 , b.length );
        }
        public int read(byte[] b, int off, int len){
            
            if ( _data == null || _offset >= _data.length ){
                
                if ( _nextChunk >= _numChunks )
                    return -1;
                
                _data = getChunk( _nextChunk );
                _offset = 0;
                _nextChunk++;
            }

            int r = Math.min( len , _data.length - _offset );
            System.arraycopy( _data , _offset , b , off , r );
            _offset += r;
            return r;
        }

        final int _numChunks;

        int _nextChunk = 0;
        int _offset;
        byte[] _data = null;
    }
    
    void remove(){
        _fs._filesCollection.remove( new BasicDBObject( "_id" , _id ) );
        _fs._chunkCollection.remove( new BasicDBObject( "files_id" , _id ) );
    }
}
