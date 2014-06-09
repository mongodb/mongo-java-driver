/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package com.mongodb.gridfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * This class enables to retrieve a GridFS file metadata and content. Operations include: - writing data to a file on disk or an
 * OutputStream - getting each chunk as a byte array - getting an InputStream to stream the data into
 * 
 * @author antoine
 * @author David Buschman
 */
public class GridFSDBFile extends GridFSFile {

    /**
     * Returns an InputStream from which data can be read.
     * 
     * @return the input stream
     */
    public InputStream getInputStream() {

        return new GridFSInputStream(this);
    }

    /**
     * Writes the file's data to a file on disk.
     * 
     * @param filename
     *            the file name on disk
     * @return number of bytes written
     * @throws IOException
     * @throws MongoException
     */
    public long writeTo(final String filename)
            throws IOException {

        return writeTo(new File(filename));
    }

    /**
     * Writes the file's data to a file on disk
     * 
     * @param f
     *            the File object
     * @return number of bytes written
     * @throws IOException
     * @throws MongoException
     */
    public long writeTo(final File f)
            throws IOException {

        FileOutputStream out = null;
        try {
            out = new FileOutputStream(f);
            return writeTo(out);
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    /**
     * Writes the file's data to an OutputStream
     * 
     * @param out
     *            the OutputStream
     * @return number of bytes written
     * @throws IOException
     * @throws MongoException
     */
    public long writeTo(final OutputStream out)
            throws IOException {

        int nc = numChunks();
        for (int i = 0; i < nc; i++) {
            out.write(getChunk(i));
        }
        return length;
    }

    byte[] getChunk(final int i) {

        if (fs == null) {
            throw new IllegalStateException("No GridFS instance defined!");
        }

        DBObject chunk = fs.getChunksCollection().findOne(new BasicDBObject("files_id", id).append("n", i));
        if (chunk == null) {
            throw new MongoException("Can't find a chunk!  file id: " + id + " chunk: " + i);
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

}
