/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// UTF8Encoding.java


//package org.postgresql.core;
package org.bson.io;

import java.io.IOException;
import java.text.MessageFormat;

class UTF8Encoding {

    private static final int MIN_2_BYTES = 0x80;
    private static final int MIN_3_BYTES = 0x800;
    private static final int MIN_4_BYTES = 0x10000;
    private static final int MAX_CODE_POINT = 0x10ffff;

    private char[] decoderArray = new char[1024];
    
    // helper for decode
    private final static void checkByte(int ch, int pos, int len) throws IOException {
        if ((ch & 0xc0) != 0x80)
            throw new IOException(MessageFormat.format("Illegal UTF-8 sequence: byte {0} of {1} byte sequence is not 10xxxxxx: {2}",
                                        new Object[] { new Integer(pos), new Integer(len), new Integer(ch) }));
    }    

    private final static void checkMinimal(int ch, int minValue) throws IOException {
        if (ch >= minValue)
            return;

        int actualLen;
        switch (minValue) {
        case MIN_2_BYTES:
            actualLen = 2;
            break;
        case MIN_3_BYTES:
            actualLen = 3;
            break;
        case MIN_4_BYTES:
            actualLen = 4;
            break;
        default:
            throw new IllegalArgumentException("unexpected minValue passed to checkMinimal: " + minValue);
        }
            
        int expectedLen;
        if (ch < MIN_2_BYTES)
            expectedLen = 1;
        else if (ch < MIN_3_BYTES)
            expectedLen = 2;
        else if (ch < MIN_4_BYTES)
            expectedLen = 3;
        else
            throw new IllegalArgumentException("unexpected ch passed to checkMinimal: " + ch);
        
        throw new IOException(MessageFormat.format("Illegal UTF-8 sequence: {0} bytes used to encode a {1} byte value: {2}",
                                    new Object[] { new Integer(actualLen), new Integer(expectedLen), new Integer(ch) }));
    }

    /**
     * Custom byte[] -> String conversion routine for UTF-8 only.
     * This is about twice as fast as using the String(byte[],int,int,String)
     * ctor, at least under JDK 1.4.2. The extra checks for illegal representations
     * add about 10-15% overhead, but they seem worth it given the number of SQL_ASCII
     * databases out there.
     *
     * @param data the array containing UTF8-encoded data
     * @param offset the offset of the first byte in <code>data</code> to decode from
     * @param length the number of bytes to decode
     * @return a decoded string
     * @throws IOException if something goes wrong
     */
    public synchronized String decode(byte[] data, int offset, int length) throws IOException {
        char[] cdata = decoderArray;
        if (cdata.length < length)
            cdata = decoderArray = new char[length];

        int in = offset;
        int out = 0;
        int end = length + offset;

        try
        {
            while (in < end)
            {
                int ch = data[in++] & 0xff;
                
                // Convert UTF-8 to 21-bit codepoint.
                if (ch < 0x80) {
                    // 0xxxxxxx -- length 1.
                } else if (ch < 0xc0) {
                    // 10xxxxxx -- illegal!
                    throw new IOException(MessageFormat.format("Illegal UTF-8 sequence: initial byte is {0}: {1}",
                                                new Object[] { "10xxxxxx", new Integer(ch) }));
                } else if (ch < 0xe0) { 
                    // 110xxxxx 10xxxxxx
                    ch = ((ch & 0x1f) << 6);
                    checkByte(data[in], 2, 2);
                    ch = ch | (data[in++] & 0x3f);
                    checkMinimal(ch, MIN_2_BYTES);
                } else if (ch < 0xf0) {
                    // 1110xxxx 10xxxxxx 10xxxxxx
                    ch = ((ch & 0x0f) << 12);
                    checkByte(data[in], 2, 3);
                    ch = ch | ((data[in++] & 0x3f) << 6);
                    checkByte(data[in], 3, 3);
                    ch = ch | (data[in++] & 0x3f);
                    checkMinimal(ch, MIN_3_BYTES);
                } else if (ch < 0xf8) {
                    // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                    ch = ((ch & 0x07) << 18);
                    checkByte(data[in], 2, 4);
                    ch = ch | ((data[in++] & 0x3f) << 12);
                    checkByte(data[in], 3, 4);
                    ch = ch | ((data[in++] & 0x3f) << 6);
                    checkByte(data[in], 4, 4);
                    ch = ch | (data[in++] & 0x3f);
                    checkMinimal(ch, MIN_4_BYTES);
                } else {
                    throw new IOException(MessageFormat.format("Illegal UTF-8 sequence: initial byte is {0}: {1}",
                                                new Object[] { "11111xxx", new Integer(ch) }));
                }
                
                if (ch > MAX_CODE_POINT)
                    throw new IOException(MessageFormat.format("Illegal UTF-8 sequence: final value is out of range: {0}",
                                                new Integer(ch)));

                // Convert 21-bit codepoint to Java chars:
                //   0..ffff are represented directly as a single char
                //   10000..10ffff are represented as a "surrogate pair" of two chars
                // See: http://java.sun.com/developer/technicalArticles/Intl/Supplementary/
                
                if (ch > 0xffff) {
                    // Use a surrogate pair to represent it.
                    ch -= 0x10000;  // ch is now 0..fffff (20 bits)
                    cdata[out++] = (char) (0xd800 + (ch >> 10));   // top 10 bits
                    cdata[out++] = (char) (0xdc00 + (ch & 0x3ff)); // bottom 10 bits
                } else if (ch >= 0xd800 && ch < 0xe000) {
                    // Not allowed to encode the surrogate range directly.
                    throw new IOException(MessageFormat.format("Illegal UTF-8 sequence: final value is a surrogate value: {0}",
                                                new Integer(ch)));
                } else {
                    // Normal case.
                    cdata[out++] = (char) ch;
                }
            }
        }
        catch (ArrayIndexOutOfBoundsException a)
        {
            throw new IOException("Illegal UTF-8 sequence: multibyte sequence was truncated");
        }

        // Check if we ran past the end without seeing an exception.
        if (in > end)
            throw new IOException("Illegal UTF-8 sequence: multibyte sequence was truncated");

        return new String(cdata, 0, out);
    }
}
