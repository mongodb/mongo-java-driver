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
    void shouldPutInt(BufferProvider provider) {
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
    void shouldPutLong(BufferProvider provider) {
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
    void shouldPutDouble(BufferProvider provider) {
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
    void shouldPutIntAtIndex(BufferProvider provider) {
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