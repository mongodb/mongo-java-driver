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

import org.testng.annotations.*;

import com.mongodb.*;
import com.mongodb.util.*;

public class GridFSTest extends TestCase {
    
    public GridFSTest()
        throws IOException , MongoException {
        super();
        try {
        cleanupMongo = new Mongo( "127.0.0.1" );
        cleanupDB = "com_mongodb_unittest_GridFSTest";
        _db = cleanupMongo.getDB( cleanupDB );
            _fs = new GridFS( _db );
        }
        catch ( MongoException e ){
            e.printStackTrace();
            throw e;
        }
        catch ( IOException io ){
            io.printStackTrace();
            throw io;
        }
        catch ( RuntimeException re ){
            re.printStackTrace();
            throw re;
        }
        catch ( Throwable t ){
            t.printStackTrace();
            throw new RuntimeException( t );
        }
    }

    int[] _get(){
        int[] i = new int[2];
        i[0] = _fs._filesCollection.find().count();
        i[1] = _fs._chunkCollection.find().count();
        return i;
    }
    
    void testInOut( String s )
        throws Exception {
        
        int[] start = _get();

        GridFSInputFile in = _fs.createFile( s.getBytes() );
        in.save();
        GridFSDBFile out = _fs.findOne( new BasicDBObject( "_id" , in.getId() ) );
        assert( out.getId().equals( in.getId() ) );
        assert( out.getChunkSize() == (long)GridFS.DEFAULT_CHUNKSIZE );
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        out.writeTo( bout );
        String outString = new String( bout.toByteArray() );
        assert( outString.equals( s ) );

        out.remove();
        int[] end = _get();
        assertEquals( start[0] , end[0] );
        assertEquals( start[1] , end[1] );
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

    void testOutStream( String s ) throws Exception {
        
        int[] start = _get();
        
        GridFSInputFile in = _fs.createFile();
        OutputStream writeStream = in.getOutputStream();
        writeStream.write( s.getBytes(), 0, s.length() );
        writeStream.close();
        GridFSDBFile out = _fs.findOne( new BasicDBObject( "_id" , in.getId() ) );
        assert ( out.getId().equals( in.getId() ) );
        assert ( out.getChunkSize() == (long) GridFS.DEFAULT_CHUNKSIZE );
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        out.writeTo( bout );
        String outString = new String( bout.toByteArray() );
        assert (outString.equals( s ));
        
        out.remove();
        int[] end = _get();
        assertEquals( start[0], end[0] );
        assertEquals( start[1], end[1] );
    }
    
    @Test(groups = { "basic" })
    public void testOutStreamSmall() throws Exception {
        testOutStream( "this is a simple test" );
    }
    
    @Test(groups = { "basic" })
    public void testOutStreamBig() throws Exception {
        int target = (int) (GridFS.DEFAULT_CHUNKSIZE * 3.5);
        StringBuilder buf = new StringBuilder( target );
        while ( buf.length() < target ) {
            buf.append( "asdasdkjasldkjasldjlasjdlajsdljasldjlasjdlkasjdlaskjdlaskjdlsakjdlaskjdasldjsad" );
        }
        String s = buf.toString();
        testOutStream( s );
    }

    @Test(groups = { "basic" })
    public void testOutStreamBigAligned() throws Exception {
        int target = (GridFS.DEFAULT_CHUNKSIZE * 4);
        StringBuilder buf = new StringBuilder( target );
        while ( buf.length() < target ) {
            buf.append( "a" );
        }
        String s = buf.toString();
        testOutStream( s );
    }

    @Test(groups = {"basic"})
    public void testMetadata()
        throws Exception {

        GridFSInputFile in = _fs.createFile( "foo".getBytes() );
        in.put("meta", 5);
        in.save();
        GridFSDBFile out = _fs.findOne( new BasicDBObject( "_id" , in.getId() ) );
        assert( out.get("meta").equals( 5 ) );
    }

    final DB _db;
    final GridFS _fs;
    
    public static void main( String args[] )
        throws Exception {
        (new GridFSTest()).runConsole();
    }

}
