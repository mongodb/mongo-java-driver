/**
 *      Copyright (C) 2008-2011 10gen Inc.
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
package org.bson.io;

import org.bson.*;

import java.io.*;
import java.nio.*;
import java.util.logging.*;

/**
 * Pseudo byte buffer, delegates as it is too hard to properly override / extend the ByteBuffer API
 *
 * @author brendan
 */
public class BSONByteBuffer {

    private BSONByteBuffer( ByteBuffer buf ){
        this.buf = buf;
        buf.order( ByteOrder.LITTLE_ENDIAN );
    }

    public static BSONByteBuffer wrap( byte[] bytes, int offset, int length ){
        return new BSONByteBuffer( ByteBuffer.wrap( bytes, offset, length ) );
    }

    public static BSONByteBuffer wrap( byte[] bytes ){
        return new BSONByteBuffer( ByteBuffer.wrap( bytes ) );
    }

    public byte get( int i ){
        return buf.get(i);
    }

    public ByteBuffer get( byte[] bytes, int offset, int length ){
        return buf.get(bytes, offset, length);
    }

    public ByteBuffer get( byte[] bytes ){
        return buf.get(bytes);
    }

    public byte[] array(){
        return buf.array();
    }

    public String toString(){
        return buf.toString();
    }

    public int hashCode(){
        return buf.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BSONByteBuffer that = (BSONByteBuffer) o;

        if (buf != null ? !buf.equals(that.buf) : that.buf != null) return false;

        return true;
    }

    /**
     * Gets a Little Endian Integer
     *
     * @param i Index to read from
     *
     * @return
     */
    public int getInt( int i ){
        return getIntLE( i );
    }

    public int getIntLE( int i ){
        int x = 0;
        x |= ( 0xFF & buf.get( i + 0 ) ) << 0;
        x |= ( 0xFF & buf.get( i + 1 ) ) << 8;
        x |= ( 0xFF & buf.get( i + 2 ) ) << 16;
        x |= ( 0xFF & buf.get( i + 3 ) ) << 24;
        return x;
    }

    public int getIntBE( int i ){
        int x = 0;
        x |= ( 0xFF & buf.get( i + 0 ) ) << 24;
        x |= ( 0xFF & buf.get( i + 1 ) ) << 16;
        x |= ( 0xFF & buf.get( i + 2 ) ) << 8;
        x |= ( 0xFF & buf.get( i + 3 ) ) << 0;
        return x;
    }

    public long getLong( int i ){
        return buf.getLong( i );
    }

    public String getCString(int offset) {
        int end = offset;
        while (get(end) != 0) {
            ++end;
        }
        int len = end - offset;
        return new String(array(), offset, len);
    }

    public String getUTF8String(int valueOffset) {
        int size = getInt(valueOffset) - 1;
        try {
            return new String(array(), valueOffset + 4, size, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new BSONException( "Cannot decode string as UTF-8." );
        }
    }

    public Buffer position( int i ){
        return buf.position(i);
    }

    public Buffer reset(){
        return buf.reset();
    }

    public int size(){
        return getInt( 0 );
    }

    protected ByteBuffer buf;
}
