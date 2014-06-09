package org.mongodb.file.writing;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.mongodb.file.MongoFile;
import org.mongodb.file.common.MongoFileConstants;

/**
 * 
 * @author David Buschman
 * 
 */
public class MongoGZipOutputStream extends OutputStream {

    private MongoFile inputFile;
    private OutputStream surrogate;

    public MongoGZipOutputStream(final MongoFile inputFile, final OutputStream given) throws IOException {

        // This chain is : me -> before -> compression -> after -> given
        //
        // It will be constructed in reverse
        //
        CountingOutputStream after = new CountingOutputStream(MongoFileConstants.compressedLength, inputFile, given);
        GZIPOutputStream compression = new GZIPOutputStream(after);
        CountingOutputStream before = new CountingOutputStream(MongoFileConstants.length, inputFile, compression);

        this.surrogate = before;
        this.inputFile = inputFile;
    }

    @Override
    public void write(final int b) throws IOException {

        this.surrogate.write(b);
    }

    @Override
    public void write(final byte[] b) throws IOException {

        this.surrogate.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {

        this.surrogate.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {

        this.surrogate.flush();
    }

    @Override
    public void close() throws IOException {

        // flush and close the streams
        try {
            this.surrogate.close();
        } catch (IOException e) {
            throw e; // re-throw it
        } finally {

            long length = inputFile.getLong(MongoFileConstants.length, 0);
            long compressed = inputFile.getLong(MongoFileConstants.compressedLength, 0);

            double ratio = 0.0d;
            if (length > 0) {
                ratio = (double) compressed / length;
            }

            inputFile.put(MongoFileConstants.compressionRatio.toString(), ratio);
            inputFile.save();
        }
    }
}
