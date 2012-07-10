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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

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
     * @throws MongoException 
     */
    public long writeTo( String filename ) throws IOException {
        return writeTo( new File( filename ) );
    }
    /**
     * Writes the file's data to a file on disk
     * @param f the File object
     * @return
     * @throws IOException
     * @throws MongoException 
     */
    public long writeTo( File f ) throws IOException {
        
    	FileOutputStream out = null;
    	try{
    		out = new FileOutputStream( f );
    		return writeTo( out);
    	}finally{
    	    if(out != null)
    	        out.close();
    	}
    }

    /**
     * Writes the file's data to an OutputStream
     * @param out the OutputStream
     * @return
     * @throws IOException
     * @throws MongoException 
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
                if ( _currentChunkIdx + 1 >= _numChunks )
                    return -1;
                
                _data = getChunk( ++_currentChunkIdx );
                _offset = 0;
            }

            int r = Math.min( len , _data.length - _offset );
            System.arraycopy( _data , _offset , b , off , r );
            _offset += r;
            return r;
        }

        /**
         * Will smartly skips over chunks without fetching them if possible.
         */
        public long skip(long numBytesToSkip) throws IOException {
            if (numBytesToSkip <= 0)
                return 0;

            if (_currentChunkIdx == _numChunks)
                //We're actually skipping over the back end of the file, short-circuit here
                //Don't count those extra bytes to skip in with the return value
                return 0;

            // offset in the whole file
            long offsetInFile = 0;
            if (_currentChunkIdx >= 0)
                offsetInFile = _currentChunkIdx * _chunkSize + _offset;
            if (numBytesToSkip + offsetInFile >= _length) {
                _currentChunkIdx = _numChunks;
                _data = null;
                return _length - offsetInFile;
            }

            int temp = _currentChunkIdx;
            _currentChunkIdx = (int)((numBytesToSkip + offsetInFile) / _chunkSize);
            if (temp != _currentChunkIdx)
                _data = getChunk(_currentChunkIdx);
            _offset = (int)((numBytesToSkip + offsetInFile) % _chunkSize);

            return numBytesToSkip;
        }

        final int _numChunks;

        int _currentChunkIdx = -1;
        int _offset = 0;
        byte[] _data = null;
    }
    
    void remove(){
        _fs._filesCollection.remove( new BasicDBObject( "_id" , _id ) );
        _fs._chunkCollection.remove( new BasicDBObject( "files_id" , _id ) );
    }
}
