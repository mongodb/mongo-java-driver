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
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

/**
 * <p>This class represents a GridFS file to be written to the database. Operations include:</p>
 *
 * <ul>
 *     <li>Writing data obtained from an InputStream</li>
 *     <li>Getting an OutputStream to stream the data out to</li>
 * </ul>
 *
 * @mongodb.driver.manual core/gridfs/ GridFS
 */
public class GridFSInputFile extends GridFSFile {

    private final InputStream inputStream;
    private final boolean closeStreamOnPersist;
    private boolean savedChunks = false;
    private byte[] buffer = null;
    private int currentChunkNumber = 0;
    private int currentBufferPosition = 0;
    private long totalBytes = 0;
    private OutputStream outputStream = null;

    /**
     * Default constructor setting the GridFS file name and providing an input stream containing data to be written to the file.
     *
     * @param gridFS               The GridFS connection handle.
     * @param inputStream          Stream used for reading data from.
     * @param filename             Name of the file to be created.
     * @param closeStreamOnPersist indicate the passed in input stream should be closed once the data chunk persisted
     */
    protected GridFSInputFile(final GridFS gridFS, final InputStream inputStream, final String filename,
                              final boolean closeStreamOnPersist) {
        this.fs = gridFS;
        this.inputStream = inputStream;
        this.filename = filename;
        this.closeStreamOnPersist = closeStreamOnPersist;

        this.id = new ObjectId();
        this.chunkSize = GridFS.DEFAULT_CHUNKSIZE;
        this.uploadDate = new Date();
        this.buffer = new byte[(int) chunkSize];
    }

    /**
     * Default constructor setting the GridFS file name and providing an input stream containing data to be written to the file.
     *
     * @param gridFS      The GridFS connection handle.
     * @param inputStream Stream used for reading data from.
     * @param filename    Name of the file to be created.
     */
    protected GridFSInputFile(final GridFS gridFS, final InputStream inputStream, final String filename) {
        this(gridFS, inputStream, filename, false);
    }

    /**
     * Constructor that only provides a file name, but does not rely on the presence of an {@link java.io.InputStream}. An {@link
     * java.io.OutputStream} can later be obtained for writing using the {@link #getOutputStream()} method.
     *
     * @param gridFS   The GridFS connection handle.
     * @param filename Name of the file to be created.
     */
    protected GridFSInputFile(final GridFS gridFS, final String filename) {
        this(gridFS, null, filename);
    }

    /**
     * Minimal constructor that does not rely on the presence of an {@link java.io.InputStream}. An {@link java.io.OutputStream} can later
     * be obtained for writing using the {@link #getOutputStream()} method.
     *
     * @param gridFS The GridFS connection handle.
     */
    protected GridFSInputFile(final GridFS gridFS) {
        this(gridFS, null, null);
    }

    /**
     * Sets the ID of this GridFS file.
     *
     * @param id the file's ID.
     */
    public void setId(final Object id) {
        this.id = id;
    }

    /**
     * Sets the file name on the GridFS entry.
     *
     * @param filename File name.
     */
    public void setFilename(final String filename) {
        this.filename = filename;
    }

    /**
     * Sets the content type (MIME type) on the GridFS entry.
     *
     * @param contentType Content type.
     */
    public void setContentType(final String contentType) {
        this.contentType = contentType;
    }

    /**
     * Set the chunk size. This must be called before saving any data.
     *
     * @param chunkSize The size in bytes.
     */
    public void setChunkSize(final long chunkSize) {
        if (outputStream != null || savedChunks) {
            return;
        }
        this.chunkSize = chunkSize;
        buffer = new byte[(int) this.chunkSize];
    }

    /**
     * Calls {@link GridFSInputFile#save(long)} with the existing chunk size.
     *
     * @throws MongoException if there's a problem saving the file.
     */
    @Override
    public void save() {
        save(chunkSize);
    }

    /**
     * This method first calls saveChunks(long) if the file data has not been saved yet. Then it persists the file entry to GridFS.
     *
     * @param chunkSize Size of chunks for file in bytes.
     * @throws MongoException if there's a problem saving the file.
     */
    public void save(final long chunkSize) {
        if (outputStream != null) {
            throw new MongoException("cannot mix OutputStream and regular save()");
        }

        // note that chunkSize only changes chunkSize in case we actually save chunks
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
     * Saves all data into chunks from configured {@link java.io.InputStream} input stream to GridFS.
     *
     * @return Number of the next chunk.
     * @throws IOException    on problems reading the new entry's {@link java.io.InputStream}.
     * @throws MongoException if there's a failure
     * @see com.mongodb.gridfs.GridFSInputFile#saveChunks(long)
     */
    public int saveChunks() throws IOException {
        return saveChunks(chunkSize);
    }

    /**
     * Saves all data into chunks from configured {@link java.io.InputStream} input stream to GridFS. A non-default chunk size can be
     * specified. This method does NOT save the file object itself, one must call save() to do so.
     *
     * @param chunkSize Size of chunks for file in bytes.
     * @return Number of the next chunk.
     * @throws IOException    on problems reading the new entry's {@link java.io.InputStream}.
     * @throws MongoException if there's a failure
     */
    public int saveChunks(final long chunkSize) throws IOException {
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
            buffer = new byte[(int) this.chunkSize];
        }

        int bytesRead = 0;
        while (bytesRead >= 0) {
            currentBufferPosition = 0;
            bytesRead = _readStream2Buffer();
            dumpBuffer(true);
        }

        // only finish data, do not write file, in case one wants to change metadata
        finishData();
        return currentChunkNumber;
    }

    /**
     * After retrieving this {@link java.io.OutputStream}, this object will be capable of accepting successively written data to the output
     * stream. To completely persist this GridFS object, you must finally call the {@link java.io.OutputStream#close()} method on the output
     * stream. Note that calling the save() and saveChunks() methods will throw Exceptions once you obtained the OutputStream.
     *
     * @return Writable stream object.
     */
    public OutputStream getOutputStream() {
        if (outputStream == null) {
            outputStream = new GridFSOutputStream();
        }
        return outputStream;
    }

    /**
     * Dumps a new chunk into the chunks collection. Depending on the flag, also partial buffers (at the end) are going to be written
     * immediately.
     *
     * @param writePartial Write also partial buffers full.
     * @throws MongoException if there's a failure
     */
    private void dumpBuffer(final boolean writePartial) {
        if ((currentBufferPosition < chunkSize) && !writePartial) {
            // Bail out, chunk not complete yet
            return;
        }
        if (currentBufferPosition == 0) {
            // chunk is empty, may be last chunk
            return;
        }

        byte[] writeBuffer = buffer;
        if (currentBufferPosition != chunkSize) {
            writeBuffer = new byte[currentBufferPosition];
            System.arraycopy(buffer, 0, writeBuffer, 0, currentBufferPosition);
        }

        DBObject chunk = createChunk(id, currentChunkNumber, writeBuffer);

        fs.getChunksCollection().save(chunk);

        currentChunkNumber++;
        totalBytes += writeBuffer.length;
        currentBufferPosition = 0;
    }

    /**
     * Creates a new chunk of this file. Can be over-ridden, if input files need to be split into chunks using a different mechanism.
     *
     * @param id                 the file ID
     * @param currentChunkNumber the unique id for this chunk
     * @param writeBuffer        the byte array containing the data for this chunk
     * @return a DBObject representing this chunk.
     */
    protected DBObject createChunk(final Object id, final int currentChunkNumber, final byte[] writeBuffer) {
        return new BasicDBObject("files_id", id)
               .append("n", currentChunkNumber)
               .append("data", writeBuffer);
    }

    /**
     * Reads a buffer full from the {@link java.io.InputStream}.
     *
     * @return Number of bytes read from stream.
     * @throws IOException if the reading from the stream fails.
     */
    private int _readStream2Buffer() throws IOException {
        int bytesRead = 0;
        while (currentBufferPosition < chunkSize && bytesRead >= 0) {
            bytesRead = inputStream.read(buffer, currentBufferPosition, (int) chunkSize - currentBufferPosition);
            if (bytesRead > 0) {
                currentBufferPosition += bytesRead;
            } else if (bytesRead == 0) {
                throw new RuntimeException("i'm doing something wrong");
            }
        }
        return bytesRead;
    }

    /**
     * Marks the data as fully written. This needs to be called before super.save()
     */
    private void finishData() {
        if (!savedChunks) {
            length = totalBytes;
            savedChunks = true;
            try {
                if (inputStream != null && closeStreamOnPersist) {
                    inputStream.close();
                }
            } catch (IOException e) {
                //ignore
            }
        }
    }

    /**
     * An output stream implementation that can be used to successively write to a GridFS file.
     */
    private class GridFSOutputStream extends OutputStream {

        @Override
        public void write(final int b) throws IOException {
            byte[] byteArray = new byte[1];
            byteArray[0] = (byte) (b & 0xff);
            write(byteArray, 0, 1);
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            int offset = off;
            int length = len;
            int toCopy = 0;
            while (length > 0) {
                toCopy = length;
                if (toCopy > chunkSize - currentBufferPosition) {
                    toCopy = (int) chunkSize - currentBufferPosition;
                }
                System.arraycopy(b, offset, buffer, currentBufferPosition, toCopy);
                currentBufferPosition += toCopy;
                offset += toCopy;
                length -= toCopy;
                if (currentBufferPosition == chunkSize) {
                    dumpBuffer(false);
                }
            }
        }

        /**
         * Processes/saves all data from {@link java.io.InputStream} and closes the potentially present {@link java.io.OutputStream}. The
         * GridFS file will be persisted afterwards.
         */
        @Override
        public void close() {
            // write last buffer if needed
            dumpBuffer(true);
            // finish stream
            finishData();
            // save file obj
            GridFSInputFile.super.save();
        }
    }
}
