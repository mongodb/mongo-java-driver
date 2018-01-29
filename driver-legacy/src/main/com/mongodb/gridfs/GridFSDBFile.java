/*
 * Copyright 2008-present MongoDB, Inc.
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
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class enables retrieving a GridFS file metadata and content. Operations include:
 * <ul>
 *     <li>Writing data to a file on disk or an OutputStream </li>
 *     <li>Creating an {@code InputStream} to stream the data into</li>
 * </ul>
 *
 * @mongodb.driver.manual core/gridfs/ GridFS
 */
public class GridFSDBFile extends GridFSFile {
    /**
     * Returns an InputStream from which data can be read.
     *
     * @return the input stream
     */
    public InputStream getInputStream() {
        return new GridFSInputStream();
    }

    /**
     * Writes the file's data to a file on disk.
     *
     * @param filename the file name on disk
     * @return number of bytes written
     * @throws IOException if there are problems writing to the file
     */
    public long writeTo(final String filename) throws IOException {
        return writeTo(new File(filename));
    }

    /**
     * Writes the file's data to a file on disk.
     *
     * @param file the File object
     * @return number of bytes written
     * @throws IOException if there are problems writing to the {@code file}
     */
    public long writeTo(final File file) throws IOException {
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
            return writeTo(out);
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    /**
     * Writes the file's data to an OutputStream.
     *
     * @param out the OutputStream
     * @return number of bytes written
     * @throws IOException if there are problems writing to {@code out}
     */
    public long writeTo(final OutputStream out) throws IOException {
        int nc = numChunks();
        for (int i = 0; i < nc; i++) {
            out.write(getChunk(i));
        }
        return length;
    }

    private byte[] getChunk(final int chunkNumber) {
        if (fs == null) {
            throw new IllegalStateException("No GridFS instance defined!");
        }

        DBObject chunk = fs.getChunksCollection().findOne(new BasicDBObject("files_id", id).append("n", chunkNumber));
        if (chunk == null) {
            throw new MongoException("Can't find a chunk!  file id: " + id + " chunk: " + chunkNumber);
        }

        return (byte[]) chunk.get("data");
    }

    /**
     * Removes file from GridFS i.e. removes documents from files and chunks collections.
     */
    void remove() {
        fs.getFilesCollection().remove(new BasicDBObject("_id", id));
        fs.getChunksCollection().remove(new BasicDBObject("files_id", id));
    }

    private class GridFSInputStream extends InputStream {

        private final int numberOfChunks;
        private int currentChunkId = -1;
        private int offset = 0;
        private byte[] buffer = null;

        GridFSInputStream() {
            this.numberOfChunks = numChunks();
        }

        @Override
        public int available() {
            if (buffer == null) {
                return 0;
            }
            return buffer.length - offset;
        }

        @Override
        public int read() {
            byte[] b = new byte[1];
            int res = read(b);
            if (res < 0) {
                return -1;
            }
            return b[0] & 0xFF;
        }

        @Override
        public int read(final byte[] b) {
            return read(b, 0, b.length);
        }

        @Override
        public int read(final byte[] b, final int off, final int len) {

            if (buffer == null || offset >= buffer.length) {
                if (currentChunkId + 1 >= numberOfChunks) {
                    return -1;
                }

                buffer = getChunk(++currentChunkId);
                offset = 0;
            }

            int r = Math.min(len, buffer.length - offset);
            System.arraycopy(buffer, offset, b, off, r);
            offset += r;
            return r;
        }

        /**
         * Will smartly skip over chunks without fetching them if possible.
         */
        @Override
        public long skip(final long bytesToSkip) throws IOException {
            if (bytesToSkip <= 0) {
                return 0;
            }

            if (currentChunkId == numberOfChunks) {
                //We're actually skipping over the back end of the file, short-circuit here
                //Don't count those extra bytes to skip in with the return value
                return 0;
            }

            // offset in the whole file
            long offsetInFile = 0;
            if (currentChunkId >= 0) {
                offsetInFile = currentChunkId * chunkSize + offset;
            }
            if (bytesToSkip + offsetInFile >= length) {
                currentChunkId = numberOfChunks;
                buffer = null;
                return length - offsetInFile;
            }

            int temp = currentChunkId;
            currentChunkId = (int) ((bytesToSkip + offsetInFile) / chunkSize);
            if (temp != currentChunkId) {
                buffer = getChunk(currentChunkId);
            }
            offset = (int) ((bytesToSkip + offsetInFile) % chunkSize);

            return bytesToSkip;
        }
    }
}
