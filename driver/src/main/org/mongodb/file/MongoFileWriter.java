package org.mongodb.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.mongodb.Document;
import org.mongodb.MongoCollection;
import org.mongodb.file.common.MongoFileConstants;
import org.mongodb.file.url.MongoFileUrl;
import org.mongodb.file.util.BytesCopier;
import org.mongodb.file.writing.BufferedChunksOutputStream;
import org.mongodb.file.writing.CountingOutputStream;
import org.mongodb.file.writing.FileChunksOutputStreamSink;
import org.mongodb.file.writing.MongoGZipOutputStream;

public class MongoFileWriter {

    private MongoFile file;
    private MongoFileUrl url;
    private MongoCollection<Document> chunksCollection;

    public MongoFileWriter(final MongoFileUrl url, final MongoFile file, final MongoCollection<Document> chunksCollection) {

        this.url = url;
        this.file = file;
        this.chunksCollection = chunksCollection;
    }

    /**
     * Stream the data to the file
     * 
     * @param in
     * @return the fileurl object
     * 
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public MongoFile write(final InputStream in) throws IOException {

        if (in == null) {
            throw new IllegalArgumentException("passed inputStream cannot be null");
        }

        // transfer the data
        OutputStream out = getOutputStream();
        try {
            new BytesCopier(in, out).transfer(true);
        } finally {
            if (out != null) {
                out.close();
            }
        }

        // make sure all the bytes transferred correctly
        file.validate();

        // return the file object
        return file;
    }

    /**
     * Returns an output stream to write data to
     * 
     * @return an OutputStream
     * @throws IOException
     */
    public OutputStream getOutputStream() throws IOException {

        MongoFileWriterAdapter adapter = new MongoFileWriterAdapter(file);

        FileChunksOutputStreamSink chunks = new FileChunksOutputStreamSink(//
                chunksCollection, file.getId(), adapter, file.getExpiresAt());

        OutputStream sink = new BufferedChunksOutputStream(chunks, file.getChunkSize());

        if (url.isStoredCompressed()) {
            return new MongoGZipOutputStream(file, sink);
        } else {
            return new CountingOutputStream(MongoFileConstants.length, file, sink);
        }
    }

    /**
     * The the MongoFile object to write to
     * 
     * @return the MongoFile object
     */
    public MongoFile getMongoFile() {

        return file;
    }

    @Override
    public String toString() {

        return String.format("MongoFileWriter [chunksCollection=%s, url=%s]", chunksCollection, url);
    }

}
