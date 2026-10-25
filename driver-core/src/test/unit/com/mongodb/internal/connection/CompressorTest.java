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

import com.github.luben.zstd.Zstd;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoInternalException;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CompressorTest {
    private static final int[] SIZES = {1, 200, 511, 512, 4096, 65_536, 1_500_000};

    static Stream<Arguments> compressors() {
        return Stream.of(
                Arguments.of("zlib", (Supplier<Compressor>) () -> new ZlibCompressor(MongoCompressor.createZlibCompressor())),
                Arguments.of("zlib level 1", (Supplier<Compressor>) () -> new ZlibCompressor(
                        MongoCompressor.createZlibCompressor().withProperty(MongoCompressor.LEVEL, 1))),
                Arguments.of("zstd", (Supplier<Compressor>) ZstdCompressor::new),
                Arguments.of("snappy", (Supplier<Compressor>) SnappyCompressor::new));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("compressors")
    @DisplayName("should round trip heap and direct buffers of every size")
    void shouldRoundTrip(final String name, final Supplier<Compressor> factory) {
        Compressor compressor = factory.get();
        for (int size : SIZES) {
            byte[] message = message(size, size);
            for (boolean directSource : new boolean[] {false, true}) {
                for (boolean directTarget : new boolean[] {false, true}) {
                    byte[] compressed = compress(compressor, split(message, 3, directSource));
                    ByteBuf target = buffer(size, directTarget);
                    compressor.uncompress(wrap(compressed, directSource), target);
                    assertEquals(0, target.remaining());
                    assertArrayEquals(message, contents(target), name + ", size " + size);
                }
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("compressors")
    @DisplayName("should leave a reused context ready for the next message")
    void shouldReuseContextsAcrossMessages(final String name, final Supplier<Compressor> factory) {
        Compressor first = factory.get();
        Compressor second = factory.get();
        for (int i = 0; i < 50; i++) {
            byte[] message = message(1 + ThreadLocalRandom.current().nextInt(20_000), i);
            Compressor sender = i % 2 == 0 ? first : second;
            Compressor receiver = i % 2 == 0 ? second : first;
            byte[] compressed = compress(sender, split(message, 1 + i % 4, false));
            ByteBuf target = buffer(message.length, false);
            receiver.uncompress(wrap(compressed, false), target);
            assertArrayEquals(message, contents(target), name + ", message " + i);
        }
    }

    @Test
    @DisplayName("zlib output should be readable by InflaterInputStream and vice versa")
    void zlibShouldBeCompatibleWithJdkStreams() throws IOException {
        Compressor compressor = new ZlibCompressor(MongoCompressor.createZlibCompressor());
        byte[] message = message(100_000, 1);

        byte[] compressed = compress(compressor, split(message, 3, false));
        assertArrayEquals(message, readAll(new InflaterInputStream(new ByteArrayInputStream(compressed))));

        ByteArrayOutputStream deflated = new ByteArrayOutputStream();
        try (DeflaterOutputStream out = new DeflaterOutputStream(deflated, new Deflater(Deflater.DEFAULT_COMPRESSION))) {
            out.write(message);
        }
        ByteBuf target = buffer(message.length, false);
        compressor.uncompress(wrap(deflated.toByteArray(), false), target);
        assertArrayEquals(message, contents(target));
    }

    @Test
    @DisplayName("zstd output should be readable by Zstd.decompress and vice versa")
    void zstdShouldBeCompatibleWithOneShotApi() {
        Compressor compressor = new ZstdCompressor();
        byte[] message = message(100_000, 2);

        byte[] compressed = compress(compressor, split(message, 3, false));
        assertArrayEquals(message, Zstd.decompress(compressed, message.length));

        ByteBuf target = buffer(message.length, false);
        compressor.uncompress(wrap(Zstd.compress(message), false), target);
        assertArrayEquals(message, contents(target));
    }

    @Test
    @DisplayName("snappy output should be readable by Snappy.uncompress and vice versa")
    void snappyShouldBeCompatibleWithRawFormat() throws IOException {
        Compressor compressor = new SnappyCompressor();
        byte[] message = message(100_000, 5);

        byte[] compressed = compress(compressor, split(message, 3, false));
        assertArrayEquals(message, Snappy.uncompress(compressed));

        ByteBuf target = buffer(message.length, false);
        compressor.uncompress(wrap(Snappy.compress(message), false), target);
        assertArrayEquals(message, contents(target));
    }

    static Stream<Arguments> checksummedCompressors() {
        return compressors().filter(arguments -> !"snappy".equals(arguments.get()[0]));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("compressors")
    @DisplayName("should reject a message whose uncompressed size differs from its header")
    void shouldRejectSizeMismatch(final String name, final Supplier<Compressor> factory) {
        Compressor compressor = factory.get();
        byte[] message = message(1000, 3);
        byte[] compressed = compress(compressor, split(message, 1, false));

        assertThrows(MongoInternalException.class, () -> compressor.uncompress(wrap(compressed, false), buffer(999, false)));
        assertThrows(MongoInternalException.class, () -> compressor.uncompress(wrap(compressed, false), buffer(1001, false)));

        ByteBuf target = buffer(message.length, false);
        compressor.uncompress(wrap(compressed, false), target);
        assertArrayEquals(message, contents(target));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("checksummedCompressors")
    @DisplayName("should reject corrupted input and keep working afterwards")
    void shouldRejectCorruptedInput(final String name, final Supplier<Compressor> factory) {
        Compressor compressor = factory.get();
        byte[] message = message(5000, 4);
        byte[] compressed = compress(compressor, split(message, 1, false));
        byte[] corrupted = Arrays.copyOf(compressed, compressed.length);
        for (int i = 2; i < corrupted.length; i += 7) {
            corrupted[i] ^= 0x5A;
        }

        assertThrows(MongoInternalException.class, () -> compressor.uncompress(wrap(corrupted, false), buffer(message.length, false)));

        ByteBuf target = buffer(message.length, false);
        compressor.uncompress(wrap(compressed, false), target);
        assertArrayEquals(message, contents(target));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("compressors")
    @DisplayName("should compress and uncompress concurrently")
    void shouldBeUsableConcurrently(final String name, final Supplier<Compressor> factory) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<Integer>> results = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                int seed = t;
                Callable<Integer> task = () -> {
                    Compressor compressor = factory.get();
                    Random random = new Random(seed);
                    for (int i = 0; i < 200; i++) {
                        byte[] message = message(1 + random.nextInt(30_000), random.nextLong());
                        byte[] compressed = compress(compressor, split(message, 1 + random.nextInt(3), random.nextBoolean()));
                        ByteBuf target = buffer(message.length, random.nextBoolean());
                        compressor.uncompress(wrap(compressed, random.nextBoolean()), target);
                        assertArrayEquals(message, contents(target));
                    }
                    return seed;
                };
                results.add(executor.submit(task));
            }
            for (Future<Integer> result : results) {
                result.get(1, TimeUnit.MINUTES);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("pool should hand out a released context again and destroy one that is not reusable")
    void poolShouldReuseOnlyReusableContexts() {
        AtomicInteger created = new AtomicInteger();
        AtomicInteger destroyed = new AtomicInteger();
        CompressionContextPool<Object> pool = new CompressionContextPool<>(() -> {
            created.incrementAndGet();
            return new Object();
        }, context -> destroyed.incrementAndGet());

        Object first = pool.get();
        pool.release(first, true);
        assertSame(first, pool.get());

        pool.release(first, false);
        assertEquals(1, destroyed.get());
        assertNotSame(first, pool.get());
        assertEquals(2, created.get());
    }

    @Test
    @DisplayName("pool should destroy contexts beyond its capacity")
    void poolShouldBeBounded() {
        AtomicInteger destroyed = new AtomicInteger();
        CompressionContextPool<Object> pool = new CompressionContextPool<>(Object::new, context -> destroyed.incrementAndGet());
        int count = Runtime.getRuntime().availableProcessors() * 2 + 5;
        List<Object> contexts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            contexts.add(pool.get());
        }
        contexts.forEach(context -> pool.release(context, true));
        assertEquals(5, destroyed.get());
    }

    private static byte[] compress(final Compressor compressor, final List<ByteBuf> source) {
        ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
        try {
            compressor.compress(source, output);
            return output.toByteArray();
        } finally {
            output.close();
        }
    }

    private static byte[] message(final int size, final long seed) {
        Random random = new Random(seed);
        String[] statuses = {"OPEN", "SETTLED", "CANCELLED", "WON", "LOST"};
        StringBuilder builder = new StringBuilder(size + 200);
        while (builder.length() < size) {
            builder.append("{_id:").append(Long.toHexString(random.nextLong()))
                    .append(",userId:").append(random.nextInt(1_000_000))
                    .append(",status:\"").append(statuses[random.nextInt(statuses.length)])
                    .append("\",stake:").append(random.nextInt(100_000)).append('}');
        }
        return Arrays.copyOf(builder.toString().getBytes(StandardCharsets.UTF_8), size);
    }

    private static List<ByteBuf> split(final byte[] message, final int parts, final boolean direct) {
        List<ByteBuf> buffers = new ArrayList<>();
        int partSize = Math.max(1, (message.length + parts - 1) / parts);
        for (int offset = 0; offset < message.length; offset += partSize) {
            buffers.add(wrap(Arrays.copyOfRange(message, offset, Math.min(message.length, offset + partSize)), direct));
        }
        return buffers;
    }

    private static ByteBuf wrap(final byte[] bytes, final boolean direct) {
        ByteBuf buffer = buffer(bytes.length, direct);
        buffer.put(bytes, 0, bytes.length);
        return buffer.flip();
    }

    private static ByteBuf buffer(final int size, final boolean direct) {
        return new ByteBufNIO(direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size));
    }

    private static byte[] contents(final ByteBuf target) {
        ByteBuf flipped = target.duplicate().flip();
        byte[] bytes = new byte[flipped.remaining()];
        flipped.get(bytes);
        return bytes;
    }

    private static byte[] readAll(final InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }
}
