/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.bson.io;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class BitsTest {

    private final byte[] BYTES = {
            41, 0, 0, 0, 16, 105, 49, 0, -12,
            -1, -1, -1, 16, 105, 50, 0, 0, 0,
            0, -128, 18, 105, 51, 0, -1, -1, -1,
            -1, -1, -1, -1, 127, 16, 105, 52, 0,
            0, 0, 0, 0, 0
    };

    @Test
    public void testReadFullyWithBufferLargerThanExpected() throws IOException {
        final byte[] buffer = new byte[8192];
        Bits.readFully(new ByteArrayInputStream(BYTES), buffer, BYTES.length);
        assertEquals(BYTES, copyOfRange(buffer, 0, BYTES.length));
    }

    @Test
     public void testReadFullyWithOffset() throws IOException {
        final int offset = 10;
        final byte[] buffer = new byte[8192];
        Bits.readFully(new ByteArrayInputStream(BYTES), buffer, offset, BYTES.length);
        assertEquals(BYTES, copyOfRange(buffer, offset, BYTES.length + offset));
    }

    @Test
    public void testReadFullyWithBufferEqualsToExpected() throws IOException {
        final int offset = 10;
        final byte[] buffer = new byte[offset+BYTES.length];
        Bits.readFully(new ByteArrayInputStream(BYTES), buffer, offset, BYTES.length);
        assertEquals(BYTES, copyOfRange(buffer, offset, BYTES.length + offset));
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testReadFullyUsingNotEnoughBigBuffer() throws IOException {
        Bits.readFully(new ByteArrayInputStream(BYTES), new byte[2], BYTES.length);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
     public void testReadFullyUsingNotEnoughBigBufferWithOffset() throws IOException {
        final int offset = 10;
        final byte[] buffer = new byte[BYTES.length];
        Bits.readFully(new ByteArrayInputStream(BYTES), buffer, offset, BYTES.length);
    }

    @Test
    public void testReadInt() {
        assertEquals(41, Bits.readInt(BYTES));
    }

    @Test
    public void testReadIntFromInputStream() throws IOException {
        assertEquals(41, Bits.readInt(new ByteArrayInputStream(BYTES)));
    }

    @Test
    public void testReadIntWithOffset() {
        assertEquals(-12, Bits.readInt(BYTES, 8));
    }

    @Test
    public void testReadIntInBigEndianNotation() {
        assertEquals(-12, Bits.readIntBE(new byte[]{-1, -1, -1, -12}, 0));
    }

    @Test
    public void testReadLong() {
        assertEquals(Long.MAX_VALUE, Bits.readLong(BYTES, 24));
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testReadLongWithNotEnoughData() {
        Bits.readLong(copyOfRange(BYTES, 24, 30), 0);
    }

    private static byte[] copyOfRange(byte[] original, int from, int to) {
        final int newLength = to - from;
        if (newLength < 0) {
            throw new IllegalArgumentException(from + " > " + to);
        }
        final byte[] copy = new byte[newLength];
        System.arraycopy(original, from, copy, 0, Math.min(original.length - from, newLength));
        return copy;
    }
}