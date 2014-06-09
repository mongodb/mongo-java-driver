package org.mongodb.file.reading;

import java.io.IOException;
import java.io.InputStream;

import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.file.MongoFile;
import org.mongodb.file.MongoFileStore;

/**
 * This class is borrowed almost verbatim from the GridFS version, not need to
 * change anything here for now.
 * 
 * @author antoine
 * @author David Buschman
 * 
 */
public class FileChunksInputStreamSource extends InputStream {

    private MongoFileStore store;
    private MongoFile file;

    private int currentChunkId = -1;
    private int offset = 0;
    private byte[] buffer = null;

    public FileChunksInputStreamSource(final MongoFileStore store, final MongoFile file) {

        this.store = store;
        this.file = file;
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
            if (currentChunkId + 1 >= file.getChunkCount()) {
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
     * Will smartly skips over chunks without fetching them if possible.
     */
    @Override
    public long skip(final long bytesToSkip) throws IOException {

        if (bytesToSkip <= 0) {
            return 0;
        }

        if (currentChunkId == file.getChunkCount()) {
            // We're actually skipping over the back end of the file,
            // short-circuit here
            // Don't count those extra bytes to skip in with the return value
            return 0;
        }

        // offset in the whole file
        long offsetInFile = 0;
        int chunkSize = file.getChunkSize();
        if (currentChunkId >= 0) {
            offsetInFile = currentChunkId * chunkSize + offset;
        }
        if (bytesToSkip + offsetInFile >= file.getLength()) {
            currentChunkId = file.getChunkCount();
            buffer = null;
            return file.getLength() - offsetInFile;
        }

        int temp = currentChunkId;
        currentChunkId = (int) ((bytesToSkip + offsetInFile) / chunkSize);
        if (temp != currentChunkId) {
            buffer = getChunk(currentChunkId);
        }
        offset = (int) ((bytesToSkip + offsetInFile) % chunkSize);

        return bytesToSkip;
    }

    byte[] getChunk(final int i) {

        if (store == null) {
            throw new IllegalStateException("No MongoFileStore instance defined!");
        }

        Document chunk = store.getChunksCollection().find(new Document("files_id", file.getId()).append("n", i)).get().next();
        if (chunk == null) {
            throw new MongoException("Can't find a chunk!  file id: " + file.getId().toString() + " chunk: " + i);
        }

        return (byte[]) chunk.get("data");
    }

}
