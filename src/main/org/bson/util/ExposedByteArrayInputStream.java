package org.bson.util;

import java.io.ByteArrayInputStream;

public class ExposedByteArrayInputStream extends ByteArrayInputStream {

    public ExposedByteArrayInputStream(byte[] arg0) {
        super( arg0 );
    }

    public ExposedByteArrayInputStream(byte[] arg0 , int arg1 , int arg2) {
        super( arg0 , arg1 , arg2 );
    }

    public byte[] getBuffer() { return this.buf; }
}
