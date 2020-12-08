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

package com.mongodb.reactivestreams.client.internal.gridfs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class ResizingByteBufferFluxTest {

    private static final String TEST_STRING = String.join("",
            asList("foobar", "foo", "bar", "baz", "qux", "quux", "quuz", "corge", "grault", "garply",
                    "waldo", "fred", "plugh", "xyzzy", "thud"));

    @TestFactory
    @DisplayName("test that the resizing publisher produces the expected results")
    List<DynamicTest> testResizingByteBufferPublisher() {
        List<DynamicTest> dynamicTests = new ArrayList<>();
        int maxByteBufferSize = 10;
        IntStream.rangeClosed(1, maxByteBufferSize).boxed().forEach(sourceSizes -> {
            int outputSizes = 1 + maxByteBufferSize - sourceSizes;
            dynamicTests.add(
                    dynamicTest("Resizing from byteBuffers of: " + sourceSizes + " to chunks of: " + outputSizes, () -> {
                        Flux<ByteBuffer> source = Flux.fromIterable(splitStringIntoChunks(TEST_STRING, sourceSizes))
                                .map(STRING_BYTE_BUFFER_FUNCTION);
                        Flux<String> output = new ResizingByteBufferFlux(source, outputSizes).map(BYTE_BUFFER_STRING_FUNCTION);
                        assertIterableEquals(splitStringIntoChunks(TEST_STRING, outputSizes), output.toIterable());
                    }));
        });
        return dynamicTests;
    }

    @Test
    public void testAndVerifyResizingByteBufferPublisher() {
        List<Long> internalRequests = new ArrayList<>();
        Flux<ByteBuffer> internal = Flux.fromIterable(asList("fo", "ob", "ar", "foo", "bar", "ba", "z"))
                .map(STRING_BYTE_BUFFER_FUNCTION)
                .doOnRequest(internalRequests::add);
        Flux<ByteBuffer> publisher = new ResizingByteBufferFlux(internal, 3);

        Duration waitDuration = Duration.ofMillis(200);
        StepVerifier.create(publisher, 0)
                .expectSubscription()
                .expectNoEvent(waitDuration)
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("foo"))
                .expectNoEvent(waitDuration)
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("bar"))
                .expectNoEvent(waitDuration)
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("foo"))
                .expectNoEvent(waitDuration)
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("bar"))
                .expectNoEvent(waitDuration)
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("baz"))
                .expectComplete()
                .verify();

        assertIterableEquals(asList(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L), internalRequests);
    }

    @Test
    public void testDirectHeapByteBuffer() {
        Flux<ByteBuffer> input = Flux.fromIterable(splitStringIntoChunks(TEST_STRING, 7))
                .map(STRING_BYTE_BUFFER_FUNCTION)
                .map(ByteBuffer::asReadOnlyBuffer);
        Flux<String> resized = new ResizingByteBufferFlux(input, 10).map(BYTE_BUFFER_STRING_FUNCTION);
        assertIterableEquals(splitStringIntoChunks(TEST_STRING, 10), resized.toIterable());
    }

    @Test
    public void testErrorsAreSignalled() {
        List<Long> internalRequests = new ArrayList<>();
        Flux<ByteBuffer> internal = Flux.fromIterable(asList("fo", "ob", "ar"))
                .map(STRING_BYTE_BUFFER_FUNCTION)
                .map(i -> {
                    if (internalRequests.size() > 2) {
                        throw new RuntimeException("Upstream error");
                    }
                    return i;
                })
                .doOnRequest(internalRequests::add);
        Flux<ByteBuffer> publisher = new ResizingByteBufferFlux(internal, 3);

        StepVerifier.create(publisher, 0)
                .expectSubscription()
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("foo"))
                .thenRequest(1)
                .expectErrorMessage("Upstream error")
                .verify();
    }

    private Collection<String> splitStringIntoChunks(final String original, final int chunkSize) {
        AtomicInteger splitCounter = new AtomicInteger(0);
        return original
                .chars()
                .mapToObj(_char -> String.valueOf((char) _char))
                .collect(Collectors.groupingBy(stringChar -> splitCounter.getAndIncrement() / chunkSize,
                        Collectors.joining()))
                .values();
    }

    static final Function<ByteBuffer, String> BYTE_BUFFER_STRING_FUNCTION = bb -> {
        ((Buffer) bb).mark();
        byte[] arr = new byte[bb.remaining()];
        bb.get(arr);
        ((Buffer) bb).reset();
        return new String(arr);
    };

    static final Function<String, ByteBuffer> STRING_BYTE_BUFFER_FUNCTION = s -> ByteBuffer.wrap(s.getBytes());

}
