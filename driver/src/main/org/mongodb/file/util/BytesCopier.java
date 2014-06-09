package org.mongodb.file.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 
 * @author David Buschman
 * 
 */
public class BytesCopier {

    private final InputStream in;
    private final OutputStream out;

    private final int blocksize;
    private boolean closeStreamOnPersist;

    public BytesCopier(final InputStream in, final OutputStream out) {

        this(8192, in, out, false);
    }

    public BytesCopier(final InputStream in, final OutputStream out, final boolean closeStreamOnPersist) {

        this(8192, in, out, closeStreamOnPersist);
    }

    public BytesCopier(final int blocksize, final InputStream in, final OutputStream out) {

        this(blocksize, in, out, false);
    }

    public BytesCopier(final int blocksize, final InputStream in, final OutputStream out, final boolean closeStreamOnPersist) {

        this.closeStreamOnPersist = closeStreamOnPersist;
        this.blocksize = blocksize;
        this.in = in;
        this.out = out;
    }

    public void transfer(final boolean flush) throws IOException {

        int nread;
        byte[] buf = new byte[blocksize];
        while ((nread = in.read(buf)) != -1) {
            out.write(buf, 0, nread);
        }
        if (flush) {
            out.flush();
        }
        if (closeStreamOnPersist) {
            in.close();
        }
    }

}