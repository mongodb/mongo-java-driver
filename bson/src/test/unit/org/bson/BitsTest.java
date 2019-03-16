/*
 * Copyright 2008-present MongoDB, Inc.
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
 *
 */

package org.bson;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BitsTest {

    private static final byte[] BYTES = {41, 0, 0, 0, 16, 105, 49, 0, -12,
                                         -1, -1, -1, 16, 105, 50, 0, 0, 0,
                                         0, -128, 18, 105, 51, 0, -1, -1, -1,
                                         -1, -1, -1, -1, 127, 16, 105, 52, 0,
                                         0, 0, 0, 0, 0};

    @Test
    public void testReadFullyWithBufferLargerThanExpected() throws IOException {
        byte[] buffer = new byte[8192];
        Bits.readFully(new ByteArrayInputStream(BYTES), buffer, 0, BYTES.length);
        assertArrayEquals(BYTES, Arrays.copyOfRange(buffer, 0, BYTES.length));
    }

    @Test
    public void testReadFullyWithOffset() throws IOException {
        int offset = 10;
        byte[] buffer = new byte[8192];
        Bits.readFully(new ByteArrayInputStream(BYTES), buffer, offset, BYTES.length);
        assertArrayEquals(BYTES, Arrays.copyOfRange(buffer, offset, BYTES.length + offset));
    }

    @Test
    public void testReadFullyWithBufferEqualsToExpected() throws IOException {
        int offset = 10;
        byte[] buffer = new byte[offset + BYTES.length];
        Bits.readFully(new ByteArrayInputStream(BYTES), buffer, offset, BYTES.length);
        assertArrayEquals(BYTES, Arrays.copyOfRange(buffer, offset, BYTES.length + offset));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadFullyUsingNotEnoughBigBuffer() throws IOException {
        Bits.readFully(new ByteArrayInputStream(BYTES), new byte[2], 0, BYTES.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadFullyUsingNotEnoughBigBufferWithOffset() throws IOException {
        int offset = 10;
        byte[] buffer = new byte[BYTES.length];
        Bits.readFully(new ByteArrayInputStream(BYTES), buffer, offset, BYTES.length);
    }

    @Test
    public void testReadInt() {
        assertEquals(41, Bits.readInt(BYTES));
    }

    @Test
    public void testReadIntFromInputStream() throws IOException {
        assertEquals(41, Bits.readInt(new ByteArrayInputStream(BYTES), new byte[4]));
    }

    @Test
    public void testReadIntWithOffset() {
        assertEquals(-12, Bits.readInt(BYTES, 8));
    }

    @Test
    public void testReadLong() {
        assertEquals(Long.MAX_VALUE, Bits.readLong(BYTES, 24));
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testReadLongWithNotEnoughData() {
        Bits.readLong(Arrays.copyOfRange(BYTES, 24, 30), 0);
    }

}
