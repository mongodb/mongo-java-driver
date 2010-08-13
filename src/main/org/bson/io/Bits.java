// Bits.java

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


package org.bson.io;

import java.io.*;

public class Bits {

    public static void readFully( InputStream in, byte[] b )
        throws IOException {
        int x = 0;
        while ( x<b.length ){
            int temp = in.read( b , x , b.length - x );
            if ( temp < 0 )
                throw new EOFException();
            x += temp;
        }
    }

    public static int readInt( InputStream in )
        throws IOException {
        byte[] data = new byte[4];
        readFully(in, data);
        return readInt(data);
    }

    public static int readInt( byte[] data ) {
        int x = 0;
        x |= ( 0xFF & data[0] ) << 0;
        x |= ( 0xFF & data[1] ) << 8;
        x |= ( 0xFF & data[2] ) << 16;
        x |= ( 0xFF & data[3] ) << 24;
        return x;
    }

    public static long readLong( InputStream in )
        throws IOException {

        byte[] data = new byte[8];
        readFully(in, data);
        return readLong(data);
    }

    public static long readLong( byte[] data ) {
        long x = 0;
        x |= ( 0xFFL & data[0] ) << 0;
        x |= ( 0xFFL & data[1] ) << 8;
        x |= ( 0xFFL & data[2] ) << 16;
        x |= ( 0xFFL & data[3] ) << 24;
        x |= ( 0xFFL & data[4] ) << 32;
        x |= ( 0xFFL & data[5] ) << 40;
        x |= ( 0xFFL & data[6] ) << 48;
        x |= ( 0xFFL & data[7] ) << 56;
        return x;
    }
}
