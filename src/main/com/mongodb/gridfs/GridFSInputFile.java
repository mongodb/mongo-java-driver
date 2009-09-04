// GridFSInputFile.java

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

public class GridFSInputFile extends GridFSFile {
    
    GridFSInputFile( GridFS fs , InputStream in , String filename ){
        _fs = fs;
        _in = in;

        _filename = filename;
        
        _id = new ObjectId();
        _chunkSize = GridFS.DEFAULT_CHUNKSIZE;
        _uploadDate = new Date();
    }

    public DBObject getMetaData(){
        if ( _metadata == null )
            _metadata = new BasicDBObject();
        return _metadata;
    }

    public void setFilename( String fn ){
        _filename = fn;
    }

    public String setContentType( String ct ){
        _contentType = ct;
    }
    
    public void save() {
        if ( ! _saved ){
            try {
                saveChunks();
            }
            catch ( IOException ioe ){
                throw new MongoException( "couldn't save chunks" , ioe );
            }
        }
        super.save();
    }

    public int saveChunks()
        throws IOException {
        if ( _saved )
            throw new RuntimeException( "already saved!" );
        
        byte[] b = new byte[GridFS.DEFAULT_CHUNKSIZE];

        long total = 0;
        int cn = 0;
        
        while ( true ){
            int start =0;
            
            while ( start < b.length ){
                int r = _in.read( b , start , b.length - start );
                if ( r == 0 )
                    throw new RuntimeException( "i'm doing something wrong" );
                if ( r < 0 )
                    break;
                start += r;
            }
            
            total += start;
            
            byte[] mine = b;
            
            if ( start != b.length ){
                mine = new byte[start];
                System.arraycopy( b , 0 , mine , 0 , start );
            }

            DBObject chunk = BasicDBObjectBuilder.start()
                .add( "files_id" , _id )
                .add( "n" , cn++ )
                .add( "data" , mine )
                .get();
            
            _fs._chunkCollection.save( chunk );
            
            if ( start < b.length )
                break;
        }
        _length = total;
        System.out.println( "length: " + _length );
        _saved = true;
        return cn;
    }
    
    final InputStream _in;
    boolean _saved = false;

}
