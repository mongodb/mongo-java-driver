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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

import org.bson.types.ObjectId;
import org.mongodb.file.MongoFileConstants;
import org.mongodb.file.util.BytesCopier;
import org.mongodb.file.writing.InputFile;

import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import com.mongodb.gridfs.io.BufferedChunksOutputStream;
import com.mongodb.gridfs.io.FileChunksOutputStreamSink;

/**
 * This class represents a GridFS file to be written to the database Operations
 * include: - writing data obtained from an InputStream - getting an
 * OutputStream to stream the data out
 * 
 * @author David Buschman
 * @author Eliot Horowitz and Guy K. Kloss
 */
public class GridFSInputFile extends GridFSFile implements InputFile {

    private final InputStream inputStream;
    private final boolean closeStreamOnPersist;
    private boolean savedChunks = false;
    private OutputStream outputStream = null;

    /**
     * Default constructor setting the GridFS file name and providing an input
     * stream containing data to be written to the file.
     * 
     * @param fs
     *            The GridFS connection handle.
     * @param in
     *            Stream used for reading data from.
     * @param filename
     *            Name of the file to be created.
     * @param closeStreamOnPersist
     *            indicate the passed in input stream should be closed once the
     *            data chunk persisted
     */
    protected GridFSInputFile(final GridFS fs, final InputStream in, final String filename, final boolean closeStreamOnPersist) {

        this.fs = fs;
        this.inputStream = in;
        this.filename = filename;
        this.closeStreamOnPersist = closeStreamOnPersist;

        this.id = new ObjectId();
        this.chunkSize = GridFS.DEFAULT_CHUNKSIZE;
        this.uploadDate = new Date();
    }

    private OutputStream generateOutputStream(final DBCollection collection) {

        GridFSInputFileAdapter adapter = new GridFSInputFileAdapter(this);

        FileChunksOutputStreamSink streamSink = new FileChunksOutputStreamSink(collection, this.id, adapter, null);

        BufferedChunksOutputStream stream = new BufferedChunksOutputStream(streamSink, this.chunkSize);
        return stream;
    }

    /**
     * Default constructor setting the GridFS file name and providing an input
     * stream containing data to be written to the file.
     * 
     * @param fs
     *            The GridFS connection handle.
     * @param in
     *            Stream used for reading data from.
     * @param filename
     *            Name of the file to be created.
     */
    protected GridFSInputFile(final GridFS fs, final InputStream in, final String filename) {

        this(fs, in, filename, false);
    }

    /**
     * Constructor that only provides a file name, but does not rely on the
     * presence of an {@link java.io.InputStream}. An
     * {@link java.io.OutputStream} can later be obtained for writing using the
     * {@link #getOutputStream()} method.
     * 
     * @param fs
     *            The GridFS connection handle.
     * @param filename
     *            Name of the file to be created.
     */
    protected GridFSInputFile(final GridFS fs, final String filename) {

        this(fs, null, filename);
    }

    /**
     * Minimal constructor that does not rely on the presence of an
     * {@link java.io.InputStream}. An {@link java.io.OutputStream} can later be
     * obtained for writing using the {@link #getOutputStream()} method.
     * 
     * @param fs
     *            The GridFS connection handle.
     */
    protected GridFSInputFile(final GridFS fs) {

        this(fs, null, null);
    }

    public void setId(final Object id) {

        this.id = id;
    }

    /**
     * Sets the file name on the GridFS entry.
     * 
     * @param filename
     *            File name.
     */
    public void setFilename(final String filename) {

        this.filename = filename;
    }

    /**
     * Sets the content type (MIME type) on the GridFS entry.
     * 
     * @param contentType
     *            Content type.
     */
    public void setContentType(final String contentType) {

        this.contentType = contentType;
    }

    /**
     * Set the chunk size. This must be called before saving any data.
     * 
     * @param chunkSize
     *            The size in bytes.
     */
    public void setChunkSize(final int chunkSize) {

        if (outputStream != null || savedChunks) {
            return;
        }
        this.chunkSize = chunkSize;
    }

    /**
     * calls {@link GridFSInputFile#save(int)} with the existing chunk size.
     * 
     * @throws MongoException
     */
    @Override
    public void save() {

        save(chunkSize);
    }

    /**
     * Internal use only!!
     */
    final void superSave() {

        super.save();
    }

    /**
     * This method first calls saveChunks(long) if the file data has not been
     * saved yet. Then it persists the file entry to GridFS.
     * 
     * @param chunkSize
     *            Size of chunks for file in bytes.
     * @throws MongoException
     */
    public void save(final int chunkSize) {

        if (outputStream != null) {
            throw new MongoException("cannot mix OutputStream and regular save()");
        }

        // note that chunkSize only changes chunkSize in case we actually save
        // chunks
        // otherwise there is a risk file and chunks are not compatible
        if (!savedChunks) {
            try {
                saveChunks(chunkSize);
            } catch (IOException ioe) {
                throw new MongoException("couldn't save chunks", ioe);
            }
        }

        super.save();
    }

    /**
     * Saves all data into chunks from configured {@link java.io.InputStream}
     * input stream to GridFS.
     * 
     * @return Number of the next chunk.
     * @throws IOException
     *             on problems reading the new entry's
     *             {@link java.io.InputStream}.
     * @throws MongoException
     */
    public int saveChunks() throws IOException {

        return saveChunks(chunkSize);
    }

    /**
     * Saves all data into chunks from configured {@link java.io.InputStream}
     * input stream to GridFS. A non-default chunk size can be specified. This
     * method does NOT save the file object itself, one must call save() to do
     * so.
     * 
     * @param chunkSize
     *            Size of chunks for file in bytes.
     * @return Number of the next chunk.
     * @throws IOException
     *             on problems reading the new entry's
     *             {@link java.io.InputStream}.
     * @throws MongoException
     */
    public int saveChunks(final int chunkSize) throws IOException {

        if (outputStream != null) {
            throw new MongoException("Cannot mix OutputStream and regular save()");
        }
        if (savedChunks) {
            throw new MongoException("Chunks already saved!");
        }

        if (chunkSize <= 0) {
            throw new MongoException("chunkSize must be greater than zero");
        }

        if (this.chunkSize != chunkSize) {
            this.chunkSize = chunkSize;
        }

        // new stuff
        this.outputStream = generateOutputStream(fs.getChunksCollection());
        try {
            new BytesCopier(inputStream, this.outputStream, this.closeStreamOnPersist).transfer(false);
        } finally {
            this.outputStream.close();
        }

        // only write data, do not write file, in case one wants to change
        // metadata
        return (int) this.getAsLong(MongoFileConstants.chunkCount.name());
    }

    public OutputStream getOutputStream() {

        if (outputStream == null) {
            outputStream = generateOutputStream(fs.getChunksCollection());
        }
        return outputStream;
    }

}
