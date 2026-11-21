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

import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SnappyCompressionTest {
    @ParameterizedTest
    @ValueSource(strings = {"empty", "slice", "two", "direct", "readOnly"})
    void emitsOneRawBlockAndConsumesTheInput(final String layout) throws IOException {
        byte[] original = {11, 21, 31, 41, 51, 61};
        ByteBuffer parent = ByteBuffer.wrap(original);
        ((Buffer) parent).position(1);
        ByteBuffer slice = parent.slice();
        ((Buffer) slice).position(1).limit(4);
        List<ByteBuf> buffers;
        byte[] expected = {31, 41, 51};
        switch (layout) {
            case "empty":
                expected = new byte[0];
                buffers = Collections.emptyList();
                break;
            case "two":
                buffers = Arrays.asList(new ByteBufNIO(ByteBuffer.wrap(original, 2, 1)), new ByteBufNIO(ByteBuffer.wrap(original, 3, 2)));
                break;
            case "direct":
                ByteBuffer direct = ByteBuffer.allocateDirect(3);
                direct.put(expected);
                ((Buffer) direct).flip();
                buffers = Collections.singletonList(new ByteBufNIO(direct));
                break;
            default:
                buffers = Collections.singletonList(new ByteBufNIO(layout.equals("readOnly") ? slice.asReadOnlyBuffer() : slice));
        }
        try (BasicOutputBuffer output = new BasicOutputBuffer()) {
            output.writeByte(17);
            new SnappyCompressor().compress(buffers, output);
            assertEquals(17, output.getInternalBuffer()[0]);
            assertArrayEquals(Snappy.compress(expected), Arrays.copyOfRange(output.toByteArray(), 1, output.getPosition()));
            buffers.forEach(buffer -> assertEquals(buffer.limit(), buffer.position()));
        } finally {
            buffers.forEach(ByteBuf::release);
        }
    }
}
