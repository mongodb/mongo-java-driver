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

    public static int readInt( InputStream in )
        throws IOException {
        int x = 0;
        x |= ( 0xFF & in.read() ) << 0;
        x |= ( 0xFF & in.read() ) << 8;
        x |= ( 0xFF & in.read() ) << 16;
        x |= ( 0xFF & in.read() ) << 24;
        return x;
    }

    public static long readLong( InputStream in )
        throws IOException {
        long x = 0;
        x |= (long)( 0xFFL & in.read() ) << 0;
        x |= (long)( 0xFFL & in.read() ) << 8;
        x |= (long)( 0xFFL & in.read() ) << 16;
        x |= (long)( 0xFFL & in.read() ) << 24;
        x |= (long)( 0xFFL & in.read() ) << 32;
        x |= (long)( 0xFFL & in.read() ) << 40;
        x |= (long)( 0xFFL & in.read() ) << 48;
        x |= (long)( 0xFFL & in.read() ) << 56;
        return x;
    }
}
