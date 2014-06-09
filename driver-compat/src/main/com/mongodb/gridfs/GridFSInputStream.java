package com.mongodb.gridfs;

import java.io.IOException;
import java.io.InputStream;

public class GridFSInputStream extends InputStream {

    private final int numberOfChunks;
    private int currentChunkId = -1;
    private int offset = 0;
    private byte[] buffer = null;
    private GridFSDBFile that;

    GridFSInputStream(final GridFSDBFile that) {

        this.that = that;
        this.numberOfChunks = that.numChunks();
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

            buffer = that.getChunk(++currentChunkId);
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

        if (currentChunkId == numberOfChunks) {
            // We're actually skipping over the back end of the file,
            // short-circuit here
            // Don't count those extra bytes to skip in with the return value
            return 0;
        }

        // offset in the whole file
        long offsetInFile = 0;
        long chunkSize = that.getChunkSize();
        if (currentChunkId >= 0) {
            offsetInFile = currentChunkId * chunkSize + offset;
        }
        if (bytesToSkip + offsetInFile >= that.getLength()) {
            currentChunkId = numberOfChunks;
            buffer = null;
            return that.getLength() - offsetInFile;
        }

        int temp = currentChunkId;
        currentChunkId = (int) ((bytesToSkip + offsetInFile) / chunkSize);
        if (temp != currentChunkId) {
            buffer = that.getChunk(currentChunkId);
        }
        offset = (int) ((bytesToSkip + offsetInFile) % chunkSize);

        return bytesToSkip;
    }

}