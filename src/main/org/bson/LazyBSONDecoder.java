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

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import org.bson.io.Bits;
import org.bson.util.ExposedByteArrayInputStream;

/**
 * implementation of BSONDecoder that creates LazyBSONObject instances
 */
public class LazyBSONDecoder implements BSONDecoder {
    static final Logger LOG = Logger.getLogger( LazyBSONDecoder.class.getName() );

    public BSONObject readObject(byte[] b) {
        try {
            return readObject( new ExposedByteArrayInputStream( b ) );
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
            return decode( new ExposedByteArrayInputStream( b ), callback );
        }
        catch ( IOException ioe ) {
            throw new BSONException( "should be impossible" , ioe );
        }
    }

    public int decode(InputStream in, BSONCallback callback) throws IOException {
        //shortcut if we don't need to copy
        if ( in instanceof ExposedByteArrayInputStream) {
            LOG.warning( "skipping stream read -> copy with ExposedByteArrayInputStream" );
            System.out.println("skipping stream read -> copy with ExposedByteArrayInputStream" );
            return decode(((ExposedByteArrayInputStream)in).getBuffer(), callback);
        }
            
        byte[] data = null;
        if (_buffer == null)
            _buffer = new byte[4096];
        int read = readBytesFully(in, _buffer, 0, 4);
        int objSize = Bits.readInt(_buffer);
        if (objSize > _buffer.length) {
            // need bigger buffer
            data = new byte[objSize];
            System.arraycopy(_buffer, 0, data, 0, read);
        } else {
            data = _buffer;
            _buffer = null;
        }
        
        if (read < objSize)
            readBytesFully(in, data, read, objSize - read);
            
        callback.gotBinary(null, (byte)0, data);
        return objSize + 4;
    }
    
    private int readBytesFully(InputStream in, byte[] buffer, int offset, int toread) throws IOException {
        int read = 0;
        int len = buffer.length;
        while (read < toread) {
            int n = in.read(buffer, offset, toread - read);
            read += n;
            offset += n;
        }
        return read;
    }

    byte[] _buffer = null;

}
