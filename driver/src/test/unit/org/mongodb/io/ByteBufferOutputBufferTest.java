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

package org.mongodb.io;

import com.mongodb.connection.ByteBufferOutputBuffer;
import org.bson.BsonSerializationException;
import org.junit.Test;
import org.mongodb.SimpleBufferProvider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByteBufferOutputBufferTest {
    private final Random random = new Random();

    @Test
    public void testBackpatch() throws IOException {
        ByteBufferOutputBuffer buf = new ByteBufferOutputBuffer(new SimpleBufferProvider());
        buf.writeInt(0);
        byte[] randomBytes = getRandomBytes(10000);
        buf.write(randomBytes, 0, 10000);

        buf.backpatchSize(randomBytes.length + 4);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        buf.pipe(out);
        byte[] bytes = out.toByteArray();
        assertEquals(randomBytes.length + 4, bytes.length);
        assertTrue(Arrays.equals(new byte[]{20, 39, 0, 0}, Arrays.copyOfRange(bytes, 0, 4)));
        assertTrue(Arrays.equals(randomBytes, Arrays.copyOfRange(bytes, 4, bytes.length)));
    }

    @Test
    public void testTruncate() throws IOException {
        ByteBufferOutputBuffer buf = new ByteBufferOutputBuffer(new SimpleBufferProvider());
        byte[] randomBytes = getRandomBytes(10000);

        buf.writeInt(0);
        buf.write(randomBytes, 0, randomBytes.length);
        buf.backpatchSize(randomBytes.length + 4);
        int pos = buf.getPosition();

        buf.write(randomBytes, 0, randomBytes.length);

        buf.truncateToPosition(pos);
        assertEquals(pos, buf.getPosition());

        byte[] randomBytesTwo = getRandomBytes(1000);
        buf.writeInt(0);
        buf.write(randomBytesTwo, 0, randomBytesTwo.length);
        buf.backpatchSize(randomBytesTwo.length + 4);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        buf.pipe(out);
        byte[] bytes = out.toByteArray();
        assertEquals(randomBytes.length + randomBytesTwo.length + 8, bytes.length);
        assertTrue(Arrays.equals(new byte[]{20, 39, 0, 0}, Arrays.copyOfRange(bytes, 0, 4)));
        assertTrue(Arrays.equals(randomBytes, Arrays.copyOfRange(bytes, 4, randomBytes.length + 4)));
        assertTrue(Arrays.equals(new byte[]{-20, 3, 0, 0}, Arrays.copyOfRange(bytes, randomBytes.length + 4, randomBytes.length + 8)));
        byte[] a21 = Arrays.copyOfRange(bytes, randomBytes.length + 8, bytes.length);
        assertTrue(Arrays.equals(randomBytesTwo, a21));
    }

    @Test(expected = BsonSerializationException.class)
    public void nullCharacterInCStringShouldThrowSerializationException() {
        ByteBufferOutputBuffer buf = new ByteBufferOutputBuffer(new SimpleBufferProvider());
        buf.writeCString("hell\u0000world");
    }

    @Test
    public void nullCharacterInStringShouldNotThrowSerializationException() {
        ByteBufferOutputBuffer buf = new ByteBufferOutputBuffer(new SimpleBufferProvider());
        buf.writeString("h\u0000i");
        assertArrayEquals(new byte[] {4, 0, 0, 0, 'h', 0, 'i', 0}, buf.toByteArray());
    }

    private byte[] getRandomBytes(final int len) {
        byte[] randomBytes = new byte[len];
        random.nextBytes(randomBytes);
        return randomBytes;
    }
}
