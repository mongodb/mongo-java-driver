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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.MongoException;
import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;

public class GridFSTest extends TestCase {
    
    public GridFSTest() {
        cleanupDB = "com_mongodb_unittest_GridFSTest";
        _db = cleanupMongo.getDB(cleanupDB);
        _fs = new GridFS(_db);
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

    @Test(groups = {"basic"})
    public void testBadChunkSize() throws Exception {
        int fileSize = 2 * _db.getMongo().getMaxBsonObjectSize();
        if (fileSize > 1024 * 1024 * 1024)
            //If this is the case, GridFS is probably obsolete...
            fileSize = 10 * 1024 * 1024;

        byte[] randomBytes = new byte[fileSize];
        for (int idx = 0; idx < fileSize; ++idx)
            randomBytes[idx] = (byte)(256 * Math.random());

        GridFSInputFile inputFile = _fs.createFile(randomBytes);
        inputFile.setFilename("bad_chunk_size.bin");
        try{
            inputFile.save(0);
            fail("should have received an exception about a chunk size being zero");
        }catch(MongoException mongoExc) {
            //We expect this exception to complain about the chunksize
            assertTrue(mongoExc.toString().contains("chunkSize must be greater than zero"));
        }

        //For good measure let's save and restore the bytes
        inputFile.save(_db.getMongo().getMaxBsonObjectSize() - 500 * 1000);
        GridFSDBFile savedFile = _fs.findOne(new BasicDBObject("_id", inputFile.getId()));
        ByteArrayOutputStream savedFileByteStream = new ByteArrayOutputStream();
        savedFile.writeTo(savedFileByteStream);
        byte[] savedFileBytes = savedFileByteStream.toByteArray();

        assertArrayEquals(randomBytes, savedFileBytes);
    }

    @Test(groups = {"basic"})
    public void getBigChunkSize() throws Exception {
        GridFSInputFile file = _fs.createFile("512kb_bucket");
        file.setChunkSize(file.getChunkSize() * 2);
        OutputStream os = file.getOutputStream();
        for (int i = 0; i < 1024; i++) {
            os.write(new byte[GridFS.DEFAULT_CHUNKSIZE / 1024 + 1]);
        }
        os.close();
   }


    @Test(groups = {"basic"})
    public void testInputStreamSkipping() throws Exception {
        //int chunkSize = 5;
        int chunkSize = GridFS.DEFAULT_CHUNKSIZE;
        int fileSize = (int)(7.25 * chunkSize);

        byte[] fileBytes = new byte[fileSize];
        for (int idx = 0; idx < fileSize; ++idx)
            fileBytes[idx] = (byte)(idx % 251);
            //Don't want chunks to be aligned at byte position 0

        GridFSInputFile inputFile = _fs.createFile(fileBytes);
        inputFile.setFilename("input_stream_skipping.bin");
        inputFile.save(chunkSize);

        GridFSDBFile savedFile = _fs.findOne(new BasicDBObject("_id", inputFile.getId()));
        GridFSDBFile.MyInputStream inputStream = (GridFSDBFile.MyInputStream)savedFile.getInputStream();

        //Quick run-through, make sure the file is as expected
        for (int idx = 0; idx < fileSize; ++idx)
            assertEquals((byte)(idx % 251), (byte)inputStream.read());

        inputStream = (GridFSDBFile.MyInputStream)savedFile.getInputStream();

        long skipped = inputStream.skip(1);
        assertEquals(1, skipped);
        int position = 1;
        assertEquals((byte)(position++ % 251), (byte)inputStream.read());

        skipped = inputStream.skip(chunkSize);
        assertEquals(chunkSize, skipped);
        position += chunkSize;
        assertEquals((byte)(position++ % 251), (byte)inputStream.read());

        skipped = inputStream.skip(-1);
        assertEquals(0, skipped);
        skipped = inputStream.skip(0);
        assertEquals(0, skipped);

        skipped = inputStream.skip(3 * chunkSize);
        assertEquals(3 * chunkSize, skipped);
        position += 3 * chunkSize;
        assertEquals((byte)(position++ % 251), (byte)inputStream.read());

        //Make sure skipping works when we skip to an exact chunk boundary
        long toSkip = inputStream.available();
        skipped = inputStream.skip(toSkip);
        assertEquals(toSkip, skipped);
        position += toSkip;
        assertEquals((byte)(position++ % 251), (byte)inputStream.read());

        skipped = inputStream.skip(2 * fileSize);
        assertEquals(fileSize - position, skipped);
        assertEquals(-1, inputStream.read());
    }

    @Test(groups = {"basic"})
    public void testCustomFileID() throws IOException {
        int chunkSize = 10;
        int fileSize = (int)(3.25 * chunkSize);

        byte[] fileBytes = new byte[fileSize];
        for (int idx = 0; idx < fileSize; ++idx)
            fileBytes[idx] = (byte)(idx % 251);

        GridFSInputFile inputFile = _fs.createFile(fileBytes);
        int id = 1;
        inputFile.setId(id);
        inputFile.setFilename("custom_file_id.bin");
        inputFile.save(chunkSize);
        assertEquals(id, inputFile.getId());

        GridFSDBFile savedFile = _fs.findOne(new BasicDBObject("_id", id));
        InputStream inputStream = savedFile.getInputStream();

        for (int idx = 0; idx < fileSize; ++idx)
            assertEquals((byte)(idx % 251), (byte)inputStream.read());
    }

    @Test(groups = {"basic"})
    public void testGetFileListWithSorting() throws Exception  {
        _fs.remove(new BasicDBObject());

        int chunkSize = 10;
        int fileSize = (int)(3.25 * chunkSize);

        byte[] fileBytes = new byte[fileSize];
        for (int idx = 0; idx < fileSize; ++idx)
            fileBytes[idx] = (byte)(idx % 251);

        GridFSInputFile inputFile;
        for (int i = 1; i < 5; i++){
            inputFile = _fs.createFile(fileBytes);
            inputFile.setId(i);
            inputFile.setFilename("custom_file_id" + i + ".bin");
            inputFile.put("orderTest", i + "");
            inputFile.save(chunkSize);
        }

        DBCursor cursor =  _fs.getFileList(null,new BasicDBObject("orderTest", 1));
        assertEquals(cursor.size(),4);
        assertEquals(cursor.next().get("_id"),1);
        assertEquals(cursor.next().get("_id"),2);
        assertEquals(cursor.next().get("_id"),3);
        assertEquals(cursor.next().get("_id"),4);

         cursor =  _fs.getFileList(null,new BasicDBObject("filename", -1));
        assertEquals(cursor.size(),4);
        assertEquals(cursor.next().get("_id"),4);
        assertEquals(cursor.next().get("_id"),3);
        assertEquals(cursor.next().get("_id"),2);
        assertEquals(cursor.next().get("_id"),1);
    }

    @Test(groups = {"basic"})
    public void testFindWithSorting() throws Exception  {
        _fs.remove(new BasicDBObject());

        int chunkSize = 10;
        int fileSize = (int)(3.25 * chunkSize);

        byte[] fileBytes = new byte[fileSize];
        for (int idx = 0; idx < fileSize; ++idx)
            fileBytes[idx] = (byte)(idx % 251);

        GridFSInputFile inputFile;
        for (int i = 1; i < 5; i++){
            inputFile = _fs.createFile(fileBytes);
            inputFile.setId(i);
            inputFile.setFilename("custom_file_id"+i+".bin");
            inputFile.put("orderTest", i+"");
            inputFile.save(chunkSize);
        }

        List<GridFSDBFile> result = _fs.find(new BasicDBObject(), new BasicDBObject("orderTest", 1));
        assertEquals(result.size(),4);
        assertEquals(result.get(0).getId(),1);
        assertEquals(result.get(1).getId(),2);
        assertEquals(result.get(2).getId(),3);
        assertEquals(result.get(3).getId(),4);

        result =  _fs.find(new BasicDBObject(), new BasicDBObject("filename", -1));
        assertEquals(result.size(),4);
        assertEquals(result.get(0).getId(),4);
        assertEquals(result.get(1).getId(),3);
        assertEquals(result.get(2).getId(),2);
        assertEquals(result.get(3).getId(),1);
    }

    final DB _db;
    final GridFS _fs;
    
    public static void main( String args[] )
        throws Exception {
        (new GridFSTest()).runConsole();
    }

}
