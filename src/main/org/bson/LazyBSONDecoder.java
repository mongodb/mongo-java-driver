/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.bson;

import org.bson.io.Bits;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * implementation of BSONDecoder that creates LazyBSONObject instances
 */
public class LazyBSONDecoder implements BSONDecoder {
    static final Logger LOG = Logger.getLogger( LazyBSONDecoder.class.getName() );

    public BSONObject readObject(byte[] b) {
        try {
            return readObject( new ByteArrayInputStream( b ) );
        }
        catch ( IOException ioe ){
            throw new BSONException( "should be impossible" , ioe );
        }
    }

    public BSONObject readObject(InputStream in) throws IOException {
        BSONCallback c = new LazyBSONCallback();
        decode( in , c );
        return (BSONObject)c.get();
    }

    public int decode(byte[] b, BSONCallback callback) {
        try {
            return decode( new ByteArrayInputStream( b ), callback );
        }
        catch ( IOException ioe ) {
            throw new BSONException( "should be impossible" , ioe );
        }
    }

    public int decode(InputStream in, BSONCallback callback) throws IOException {
        byte[] objSizeBuffer = new byte[BYTES_IN_INTEGER];
        Bits.readFully(in, objSizeBuffer, 0, BYTES_IN_INTEGER);
        int objSize = Bits.readInt(objSizeBuffer);
        byte[] data = new byte[objSize];
        System.arraycopy(objSizeBuffer, 0, data, 0, BYTES_IN_INTEGER);

        Bits.readFully(in, data, BYTES_IN_INTEGER, objSize - BYTES_IN_INTEGER);

        // note that we are handing off ownership of the data byte array to the callback
        callback.gotBinary(null, (byte) 0, data);
        return objSize;
    }

    private static int BYTES_IN_INTEGER = 4;
}
