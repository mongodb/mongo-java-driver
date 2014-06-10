package org.mongodb.file.writing;

import java.io.IOException;
import java.io.OutputStream;

import org.mongodb.file.MongoFile;
import org.mongodb.file.MongoFileConstants;

public class CountingOutputStream extends OutputStream {

    private long count = 0;
    private MongoFileConstants key;
    private MongoFile inputFile;
    private OutputStream out;

    public CountingOutputStream(final MongoFileConstants key, final MongoFile inputFile, final OutputStream out) {

        this.key = key;
        this.inputFile = inputFile;
        this.out = out;
    }

    @Override
    public void write(final int b) throws IOException {

        out.write(b);
        ++count;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {

        out.write(b, off, len);
        count += len;
    }

    @Override
    public void close() throws IOException {

        out.close();

        inputFile.put(key.toString(), count);
        inputFile.save();
    }

    public long getCount() {
        return count;
    }
}
