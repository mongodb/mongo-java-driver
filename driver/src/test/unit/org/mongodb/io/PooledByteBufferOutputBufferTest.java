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

package org.mongodb.io;

import org.bson.BSONSerializationException;
import org.junit.Test;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.impl.PowerOfTwoBufferPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PooledByteBufferOutputBufferTest {
    private final Random random = new Random();

    @Test
    public void testBackpatch() throws IOException {
        final PooledByteBufferOutputBuffer buf = new PooledByteBufferOutputBuffer(new PowerOfTwoBufferPool(11));
        buf.writeInt(0);
        final byte[] randomBytes = getRandomBytes(10000);
        buf.write(randomBytes, 0, 10000);

        buf.backpatchSize(randomBytes.length + 4);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        buf.pipe(out);
        final byte[] bytes = out.toByteArray();
        assertEquals(randomBytes.length + 4, bytes.length);
        assertTrue(Arrays.equals(new byte[]{20, 39, 0, 0}, Arrays.copyOfRange(bytes, 0, 4)));
        assertTrue(Arrays.equals(randomBytes, Arrays.copyOfRange(bytes, 4, bytes.length)));
    }

    @Test
    public void testTruncate() throws IOException {
        final PooledByteBufferOutputBuffer buf = new PooledByteBufferOutputBuffer(new PowerOfTwoBufferPool(11));
        final byte[] randomBytes = getRandomBytes(10000);

        buf.writeInt(0);
        buf.write(randomBytes, 0, randomBytes.length);
        buf.backpatchSize(randomBytes.length + 4);
        final int pos = buf.getPosition();

        buf.write(randomBytes, 0, randomBytes.length);

        buf.truncateToPosition(pos);
        assertEquals(pos, buf.getPosition());

        final byte[] randomBytesTwo = getRandomBytes(1000);
        buf.writeInt(0);
        buf.write(randomBytesTwo, 0, randomBytesTwo.length);
        buf.backpatchSize(randomBytesTwo.length + 4);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        buf.pipe(out);
        final byte[] bytes = out.toByteArray();
        assertEquals(randomBytes.length + randomBytesTwo.length + 8, bytes.length);
        assertTrue(Arrays.equals(new byte[] {20, 39, 0, 0}, Arrays.copyOfRange(bytes, 0, 4)));
        assertTrue(Arrays.equals(randomBytes, Arrays.copyOfRange(bytes, 4, randomBytes.length + 4)));
        assertTrue(Arrays.equals(new byte[]{-20, 3, 0, 0}, Arrays.copyOfRange(bytes, randomBytes.length + 4, randomBytes.length + 8)));
        final byte[] a21 = Arrays.copyOfRange(bytes, randomBytes.length + 8, bytes.length);
        assertTrue(Arrays.equals(randomBytesTwo, a21));
    }

    @Test(expected = BSONSerializationException.class)
    public void testNullCharacterInCString() {
        final PooledByteBufferOutputBuffer buf = new PooledByteBufferOutputBuffer(new PowerOfTwoBufferPool(11));
        buf.writeCString("hell\u0000world");
    }

    private byte[] getRandomBytes(final int len) {
        final byte[] randomBytes = new byte[len];
        random.nextBytes(randomBytes);
        return randomBytes;
    }
}
