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
 */

package com.mongodb.internal.connection;

import com.mongodb.MongoInternalException;
import com.mongodb.internal.connection.netty.NettyByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnappyDecompressionTest {
    private final SnappyCompressor compressor = new SnappyCompressor();

    @ParameterizedTest
    @ValueSource(strings = {"heap", "direct", "readOnlyHeap", "readOnlyDirect", "mixed"})
    void decodesIntoDestinationSlice(final String layout) throws IOException {
        byte[] data = {10, 20, 30, 40};
        byte[] compressed = Snappy.compress(data);
        boolean direct = layout.equals("direct") || layout.equals("readOnlyDirect");
        ByteBuffer input = slice(compressed.length, direct);
        input.put(compressed);
        ((Buffer) input).position(2);
        ByteBuf source = new ByteBufNIO(layout.startsWith("readOnly") ? input.asReadOnlyBuffer() : input);
        ByteBuffer output = slice(data.length, direct || layout.equals("mixed"));
        ByteBuf target = new ByteBufNIO(output);
        try {
            compressor.uncompress(source, target);
            assertEquals(source.limit(), source.position());
            assertEquals(2 + data.length, target.position());
            assertEquals(2 + data.length, target.limit());
            assertEquals(0, output.get(1));
            ((Buffer) output).position(2);
            byte[] result = new byte[data.length];
            output.get(result);
            assertArrayEquals(data, result);
        } finally {
            target.release();
            source.release();
        }
    }

    private static ByteBuffer slice(final int length, final boolean direct) {
        ByteBuffer parent = direct ? ByteBuffer.allocateDirect(length + 16) : ByteBuffer.allocate(length + 16);
        ((Buffer) parent).position(5);
        ByteBuffer view = parent.slice();
        ((Buffer) view).position(2).limit(2 + length);
        return view;
    }

    @Test
    void decodesIntoBothComponentsOfANettyDestination() throws IOException {
        byte[] expected = {1, 2, 3, 4};
        byte[] bytes = Snappy.compress(expected);
        ByteBuffer encoded = ByteBuffer.allocateDirect(bytes.length);
        encoded.put(bytes);
        ((Buffer) encoded).flip();
        ByteBuf input = new ByteBufNIO(encoded);
        CompositeByteBuf composite = Unpooled.compositeBuffer();
        composite.addComponents(false, Unpooled.directBuffer(2).writeZero(2), Unpooled.directBuffer(2).writeZero(2));
        ByteBuf target = new NettyByteBuf(composite);
        try {
            compressor.uncompress(input, target);
            byte[] decoded = new byte[4];
            target.flip().get(decoded);
            assertArrayEquals(expected, decoded);
        } finally {
            input.release();
            target.release();
        }
    }

    @Test
    void acceptsAValidEmptyBlock() throws IOException {
        ByteBuf input = new ByteBufNIO(ByteBuffer.wrap(Snappy.compress(new byte[0])));
        ByteBuf output = new ByteBufNIO(ByteBuffer.allocate(0));
        try {
            compressor.uncompress(input, output);
            assertEquals(input.limit(), input.position());
            assertEquals(0, output.position());
        } finally {
            input.release();
            output.release();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejectsSizeMismatchOrIncompleteBlock(final boolean direct) throws IOException {
        byte[] valid = Snappy.compress(new byte[]{1, 2, 3, 4});
        assertInvalid(valid, 3, direct);
        assertInvalid(valid, 5, direct);
        assertInvalid(new byte[0], 4, direct);
        assertInvalid(Arrays.copyOf(valid, valid.length - 1), 4, direct);
    }

    private void assertInvalid(final byte[] raw, final int expected, final boolean direct) {
        ByteBuffer input = slice(raw.length, direct);
        input.put(raw);
        ((Buffer) input).position(2);
        ByteBuf source = new ByteBufNIO(input);
        ByteBuf target = new ByteBufNIO(slice(expected, direct));
        try {
            assertThrows(MongoInternalException.class, () -> compressor.uncompress(source, target));
            assertEquals(2, target.position());
            assertEquals(2, source.position());
        } finally {
            source.release();
            target.release();
        }
    }
}
