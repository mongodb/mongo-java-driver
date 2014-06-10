package com.mongodb.gridfs.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

import org.mongodb.file.MongoFileConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

/**
 * A sink object the absorbs all the chunks that are sent to it and create
 * MongoDB chunks for each one.
 * 
 * Place this object behind the BufferedChunksOutputStream
 * 
 * @author David Buschman
 * 
 */
public class FileChunksOutputStreamSink extends OutputStream {

    private Object id;
    private DBCollection collection;
    private int currentChunkNumber = 0;
    private ChunksStatisticsAdapter adapter;
    private Date expiresAt;

    public FileChunksOutputStreamSink(final DBCollection collection, final Object fileId, final ChunksStatisticsAdapter adapter,
            final Date expiresAt) {

        this.collection = collection;
        this.id = fileId;
        this.adapter = adapter;
        this.expiresAt = expiresAt;
    }

    @Override
    public void write(final int b) throws IOException {

        throw new IllegalStateException("Single byte writing not supported with this OutputStream");
    }

    @Override
    public void write(final byte[] b) throws IOException {

        if (b == null) {
            throw new IllegalArgumentException("buffer cannot be null");
        }

        super.write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] buffer, final int offset, final int length) throws IOException {

        byte[] internal = buffer; // assume the whole passed in buffer for
                                  // efficiency

        // if partial buffer, then we have to copy the data until serialized
        if (offset != 0 || length != buffer.length) {
            internal = new byte[length];
            System.arraycopy(buffer, offset, internal, 0, length);
        }

        // construct the chunk
        BasicDBObject dbObject = new BasicDBObject("files_id", id)//
                .append("n", currentChunkNumber)// Sequence number of the chunk
                                                // in the file
                .append("sz", length)// length of the chunk data portion on the
                                     // chunk
                .append("data", internal); // the data encoded

        if (expiresAt != null) {
            dbObject.put(MongoFileConstants.expireAt.toString(), expiresAt);
        }
        ++currentChunkNumber;

        // persist it
        collection.save(dbObject);
        adapter.collectFromChunk(dbObject);
    }

    @Override
    public void flush() throws IOException {

        adapter.flush();
    }

    @Override
    public void close() throws IOException {

        adapter.close();
    }

}
