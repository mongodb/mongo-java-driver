package org.mongodb.file.reading;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.mongodb.file.MongoFile;
import org.mongodb.file.MongoFileConstants;

public class CountingInputStream extends FilterInputStream {

    private long count = 0;
    // private MessageDigest messageDigest;
    private MongoFile file;

    public CountingInputStream(final MongoFile file, final InputStream given) {

        super(given);
        this.file = file;

        // try {
        // this.messageDigest = MessageDigest.getInstance("MD5");
        // } catch (NoSuchAlgorithmException e) {
        // throw new RuntimeException("No MD5!");
        // }
    }

    @Override
    public int read() throws IOException {

        int read = super.read();
        if (read > 0) {
            count += read;
            // messageDigest.update((byte) read);
        }
        return read;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {

        int read = super.read(b, off, len);
        if (read > 0) {
            count += read;
            // messageDigest.update(b, off, read);
        }
        return read;
    }

    public final long getCount() {

        return count;
    }

    @Override
    public void close() throws IOException {
        super.close();

        // check for full file read
        long expected = file.isCompressed() ? file.getLong(MongoFileConstants.compressedLength) : file.getLength();
        if (expected != count) {
            throw new IOException("File Length mispatch");
        }

    }

}
