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

import java.util.*;
import java.util.regex.*;
import java.io.*;

import org.testng.annotations.Test;

import com.mongodb.*;
import com.mongodb.gridfs.*;
import com.mongodb.util.*;

public class GridFSTest extends TestCase {

    public GridFSTest()
        throws IOException , MongoException {
        super();
        _db = new Mongo( "127.0.0.1" , "cursortest" );
        _fs = new GridFS( _db );
    }
    
    void testInOut( String s )
        throws Exception {
        GridFSInputFile in = _fs.createFile( s.getBytes() );
        in.save();
        GridFSDBFile out = _fs.findOne( new BasicDBObject( "_id" , in.getId() ) );
        assert( out.getId().equals( in.getId() ) );
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        out.writeTo( bout );
        String outString = new String( bout.toByteArray() );
        assert( outString.equals( s ) );
        assert( false );
    }
    
    @Test(groups = {"basic"})
    public void testSmall()
        throws Exception {
        testInOut( "this is a simple test" );
    }

    @Test(groups = {"basic"})
    public void testBig()
        throws Exception {
        int target = GridFS.DEFAULT_CHUNKSIZE * 3;
        StringBuilder buf = new StringBuilder( target );
        while ( buf.length() < target )
            buf.append( "asdasdkjasldkjasldjlasjdlajsdljasldjlasjdlkasjdlaskjdlaskjdlsakjdlaskjdasldjsad" );
        String s = buf.toString();
        testInOut( s );
    }
    
    final Mongo _db;
    final GridFS _fs;
    
    public static void main( String args[] )
        throws Exception {
        (new GridFSTest()).runConsole();
    }

}
