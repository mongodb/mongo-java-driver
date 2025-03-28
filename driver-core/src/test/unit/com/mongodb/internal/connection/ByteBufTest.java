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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;


class ByteBufTest {

    static Stream<BufferProvider> bufferProviders() {
        return Stream.of(new ByteBufSpecification.NettyBufferProvider(), new SimpleBufferProvider());
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldPutInt(final BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(1024);
        try {
            buffer.putInt(42);
            buffer.flip();
            assertEquals(42, buffer.getInt());
        } finally {
            buffer.release();
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldPutLong(final BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(1024);
        try {
            buffer.putLong(42L);
            buffer.flip();
            assertEquals(42L, buffer.getLong());
        } finally {
            buffer.release();
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldPutDouble(final BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(1024);
        try {
            buffer.putDouble(42.0D);
            buffer.flip();
            assertEquals(42.0D, buffer.getDouble());
        } finally {
            buffer.release();
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldPutIntAtIndex(final BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(1024);
        try {
            buffer.putInt(0);
            buffer.putInt(0);
            buffer.putInt(0);
            buffer.putInt(0);
            buffer.put((byte) 43);
            buffer.put((byte) 44);
            buffer.putInt(0, 22);
            buffer.putInt(4, 23);
            buffer.putInt(8, 24);
            buffer.putInt(12, 25);
            buffer.flip();

            assertEquals(22, buffer.getInt());
            assertEquals(23, buffer.getInt());
            assertEquals(24, buffer.getInt());
            assertEquals(25, buffer.getInt());
            assertEquals(43, buffer.get());
            assertEquals(44, buffer.get());
        } finally {
            buffer.release();
        }
    }
}
