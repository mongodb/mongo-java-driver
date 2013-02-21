/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
import com.mongodb.DatabaseTestCase;
import com.mongodb.MongoException;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.nio.charset.Charset.defaultCharset;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GridFSTest extends DatabaseTestCase {
    private GridFS gridFS;

    @Before
    public void setUp() {
        super.setUp();
        gridFS = new GridFS(database);
    }

    @Test
    public void testSmall() throws Exception {
        testInOut("this is a simple test");
    }

    @Test
    public void testBig() throws Exception {
        final int target = GridFS.DEFAULT_CHUNKSIZE * 3;
        final StringBuilder buf = new StringBuilder(target);
        while (buf.length() < target) {
            buf.append("asdasdkjasldkjasldjlasjdlajsdljasldjlasjdlkasjdlaskjdlaskjdlsakjdlaskjdasldjsad");
        }
        final String s = buf.toString();
        testInOut(s);
    }

    void testOutStream(final String s) throws Exception {

        final int[] start = _get();

        final GridFSInputFile in = gridFS.createFile();
        final OutputStream writeStream = in.getOutputStream();
        writeStream.write(s.getBytes(defaultCharset()), 0, s.length());
        writeStream.close();
        final GridFSDBFile out = gridFS.findOne(new BasicDBObject("_id", in.getId()));
        assert (out.getId().equals(in.getId()));
        assert (out.getChunkSize() == (long) GridFS.DEFAULT_CHUNKSIZE);

        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        out.writeTo(bout);
        final String outString = new String(bout.toByteArray(), defaultCharset());
        assert (outString.equals(s));

        out.remove();
        final int[] end = _get();
        assertEquals(start[0], end[0]);
        assertEquals(start[1], end[1]);
    }

    @Test
    public void testOutStreamSmall() throws Exception {
        testOutStream("this is a simple test");
    }

    @Test
    public void testOutStreamBig() throws Exception {
        final int target = (int) (GridFS.DEFAULT_CHUNKSIZE * 3.5);
        final StringBuilder buf = new StringBuilder(target);
        while (buf.length() < target) {
            buf.append("asdasdkjasldkjasldjlasjdlajsdljasldjlasjdlkasjdlaskjdlaskjdlsakjdlaskjdasldjsad");
        }
        final String s = buf.toString();
        testOutStream(s);
    }

    @Test
    public void testOutStreamBigAligned() throws Exception {
        final int target = (GridFS.DEFAULT_CHUNKSIZE * 4);
        final StringBuilder buf = new StringBuilder(target);
        while (buf.length() < target) {
            buf.append("a");
        }
        final String s = buf.toString();
        testOutStream(s);
    }

    @Test
    public void testMetadata() throws Exception {

        final GridFSInputFile in = gridFS.createFile("foo".getBytes(defaultCharset()));
        in.put("meta", 5);
        in.save();
        final GridFSDBFile out = gridFS.findOne(new BasicDBObject("_id", in.getId()));
        assert (out.get("meta").equals(5));
    }

    @Test
    public void testBadChunkSize() throws Exception {
        int fileSize = (int) (2 * GridFS.MAX_CHUNKSIZE);
        if (fileSize > 1024 * 1024 * 1024) {
            //If this is the case, GridFS is probably obsolete...
            fileSize = 10 * 1024 * 1024;
        }

        final byte[] randomBytes = new byte[fileSize];
        for (int idx = 0; idx < fileSize; ++idx) {
            randomBytes[idx] = (byte) (256 * Math.random());
        }

        final GridFSInputFile inputFile = gridFS.createFile(randomBytes);
        inputFile.setFilename("bad_chunk_size.bin");
        try {
            inputFile.save(0);
            fail("should have received an exception about a chunk size being zero");
        } catch (MongoException mongoExc) {
            //We expect this exception to complain about the chunksize
            assertTrue(mongoExc.toString().contains("chunkSize must be greater than zero"));
        }

        try {
            inputFile.save(GridFS.MAX_CHUNKSIZE + 10);
            fail("should have received an exception about a chunk size being too big");
        } catch (MongoException mongoExc) {
            //also expecting it to complain about the chunkSize
            assertTrue(mongoExc.toString().contains("and less than or equal to GridFS.MAX_CHUNKSIZE"));
        }

        //For good measure let's save and restore the bytes
        inputFile.save(GridFS.MAX_CHUNKSIZE / 2);
        final GridFSDBFile savedFile = gridFS.findOne(new BasicDBObject("_id", inputFile.getId()));
        final ByteArrayOutputStream savedFileByteStream = new ByteArrayOutputStream();
        savedFile.writeTo(savedFileByteStream);
        final byte[] savedFileBytes = savedFileByteStream.toByteArray();

        assertArrayEquals(randomBytes, savedFileBytes);
    }

    @Test
    public void getBigChunkSize() throws Exception {
        final GridFSInputFile file = gridFS.createFile("512kb_bucket");
        file.setChunkSize(file.getChunkSize() * 2);
        final OutputStream os = file.getOutputStream();
        for (int i = 0; i < 1024; i++) {
            os.write(new byte[GridFS.DEFAULT_CHUNKSIZE / 1024 + 1]);
        }
        os.close();
    }


    @Test
    public void testInputStreamSkipping() throws Exception {
        //int chunkSize = 5;
        final int chunkSize = GridFS.DEFAULT_CHUNKSIZE;
        final int fileSize = (int) (7.25 * chunkSize);

        final byte[] fileBytes = new byte[fileSize];
        for (int idx = 0; idx < fileSize; ++idx) {
            fileBytes[idx] = (byte) (idx % 251);
        }
        //Don't want chunks to be aligned at byte position 0

        final GridFSInputFile inputFile = gridFS.createFile(fileBytes);
        inputFile.setFilename("input_stream_skipping.bin");
        inputFile.save(chunkSize);

        final GridFSDBFile savedFile = gridFS.findOne(new BasicDBObject("_id", inputFile.getId()));
        GridFSDBFile.MyInputStream inputStream = (GridFSDBFile.MyInputStream) savedFile.getInputStream();

        //Quick run-through, make sure the file is as expected
        for (int idx = 0; idx < fileSize; ++idx) {
            assertEquals((byte) (idx % 251), (byte) inputStream.read());
        }

        inputStream = (GridFSDBFile.MyInputStream) savedFile.getInputStream();

        long skipped = inputStream.skip(1);
        assertEquals(1, skipped);
        int position = 1;
        assertEquals((byte) (position++ % 251), (byte) inputStream.read());

        skipped = inputStream.skip(chunkSize);
        assertEquals(chunkSize, skipped);
        position += chunkSize;
        assertEquals((byte) (position++ % 251), (byte) inputStream.read());

        skipped = inputStream.skip(-1);
        assertEquals(0, skipped);
        skipped = inputStream.skip(0);
        assertEquals(0, skipped);

        skipped = inputStream.skip(3 * chunkSize);
        assertEquals(3 * chunkSize, skipped);
        position += 3 * chunkSize;
        assertEquals((byte) (position++ % 251), (byte) inputStream.read());

        //Make sure skipping works when we skip to an exact chunk boundary
        final long toSkip = inputStream.available();
        skipped = inputStream.skip(toSkip);
        assertEquals(toSkip, skipped);
        position += toSkip;
        assertEquals((byte) (position++ % 251), (byte) inputStream.read());

        skipped = inputStream.skip(2 * fileSize);
        assertEquals(fileSize - position, skipped);
        assertEquals(-1, inputStream.read());
    }

    @Test
    public void testCustomFileID() throws IOException {
        final int chunkSize = 10;
        final int fileSize = (int) (3.25 * chunkSize);

        final byte[] fileBytes = new byte[fileSize];
        for (int idx = 0; idx < fileSize; ++idx) {
            fileBytes[idx] = (byte) (idx % 251);
        }

        final GridFSInputFile inputFile = gridFS.createFile(fileBytes);
        final int id = 1;
        inputFile.setId(id);
        inputFile.setFilename("custom_file_id.bin");
        inputFile.save(chunkSize);
        assertEquals(id, inputFile.getId());

        final GridFSDBFile savedFile = gridFS.findOne(new BasicDBObject("_id", id));
        final InputStream inputStream = savedFile.getInputStream();

        for (int idx = 0; idx < fileSize; ++idx) {
            assertEquals((byte) (idx % 251), (byte) inputStream.read());
        }
    }

    int[] _get() {
        final int[] i = new int[2];
        i[0] = gridFS._filesCollection.find().count();
        i[1] = gridFS._chunkCollection.find().count();
        return i;
    }

    void testInOut(final String s) throws Exception {

        final int[] start = _get();

        final GridFSInputFile in = gridFS.createFile(s.getBytes(defaultCharset()));
        in.save();
        final GridFSDBFile out = gridFS.findOne(new BasicDBObject("_id", in.getId()));
        assert (out.getId().equals(in.getId()));
        assert (out.getChunkSize() == (long) GridFS.DEFAULT_CHUNKSIZE);

        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        out.writeTo(bout);
        final String outString = new String(bout.toByteArray(), defaultCharset());
        assert (outString.equals(s));

        out.remove();
        final int[] end = _get();
        assertEquals(start[0], end[0]);
        assertEquals(start[1], end[1]);
    }
}
