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

package com.mongodb.connection.netty;

import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.ResourceUtil;
import com.mongodb.internal.connection.Stream;
import com.mongodb.internal.connection.netty.NettyByteBuf;
import com.mongodb.internal.connection.netty.NettyStreamFactory;
import com.mongodb.spi.dns.InetAddressResolver;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCounted;
import org.bson.ByteBuf;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getSslSettings;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


@SuppressWarnings("deprecation")
@DisplayName("NettyStream - Connection, Validation & Leak Prevention Tests")
class NettyStreamTest {

    private NioEventLoopGroup eventLoopGroup;
    private NettyStreamFactory factory;
    private Stream stream;
    private TrackingNettyByteBufAllocator trackingAllocator;

    @BeforeEach
    void setUp() {
        eventLoopGroup = new NioEventLoopGroup();
        trackingAllocator = new TrackingNettyByteBufAllocator();
    }

    @AfterEach
    void tearDown() {
        if (stream != null && !stream.isClosed()) {
            stream.close();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        }
        System.gc();
    }

    private static boolean isSslEnabled() {
        return getSslSettings().isEnabled();
    }

    // ========== Original Tests Converted from Spock ==========

    @Test
    @Tag("Slow")
    @DisabledIf("isSslEnabled")
    @DisplayName("Should successfully connect when at least one IP address in the group is reachable")
    void shouldSuccessfullyConnectWithWorkingIpAddressGroup() throws IOException {
        // given
        SocketSettings socketSettings = SocketSettings.builder()
                .connectTimeout(1000, TimeUnit.MILLISECONDS)
                .build();
        SslSettings sslSettings = SslSettings.builder().build();

        InetAddressResolver inetAddressResolver = host -> {
            try {
                return asList(
                        InetAddress.getByName("192.168.255.255"),
                        InetAddress.getByName("1.2.3.4"),
                        InetAddress.getByName("127.0.0.1")
                );
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        };

        factory = new NettyStreamFactory(inetAddressResolver, socketSettings, sslSettings,
                eventLoopGroup, NioSocketChannel.class, trackingAllocator, null);
        stream = factory.create(new ServerAddress());

        // when
        stream.open(OPERATION_CONTEXT);

        // then
        assertFalse(stream.isClosed());

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @Tag("Slow")
    @DisabledIf("isSslEnabled")
    @DisplayName("Should throw MongoSocketOpenException when all IP addresses in the group are unreachable")
    void shouldThrowExceptionWithNonWorkingIpAddressGroup() {
        // given
        SocketSettings socketSettings = SocketSettings.builder()
                .connectTimeout(1000, TimeUnit.MILLISECONDS)
                .build();
        SslSettings sslSettings = SslSettings.builder().build();

        InetAddressResolver inetAddressResolver = host -> {
            try {
                return asList(
                        InetAddress.getByName("192.168.255.255"),
                        InetAddress.getByName("1.2.3.4"),
                        InetAddress.getByName("1.2.3.5")
                );
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        };

        factory = new NettyStreamFactory(inetAddressResolver, socketSettings, sslSettings,
                eventLoopGroup, NioSocketChannel.class, trackingAllocator, null);
        stream = factory.create(new ServerAddress());

        // when/then
        assertThrows(MongoSocketOpenException.class, () -> stream.open(OPERATION_CONTEXT));

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should fail AsyncCompletionHandler when DNS name resolution fails")
    void shouldFailAsyncCompletionHandlerIfNameResolutionFails() throws InterruptedException {
        // given
        ServerAddress serverAddress = new ServerAddress("nonexistent.invalid.hostname.test");
        MongoSocketException exception = new MongoSocketException("Temporary failure in name resolution", serverAddress);

        InetAddressResolver inetAddressResolver = host -> {
            throw exception;
        };

        SocketSettings socketSettings = SocketSettings.builder()
                .connectTimeout(1000, TimeUnit.MILLISECONDS)
                .build();
        SslSettings sslSettings = SslSettings.builder().build();

        factory = new NettyStreamFactory(inetAddressResolver, socketSettings, sslSettings,
                eventLoopGroup, NioSocketChannel.class, trackingAllocator, null);
        stream = factory.create(serverAddress);

        CallbackErrorHolder<Void> callback = new CallbackErrorHolder<>();

        // when
        stream.openAsync(OPERATION_CONTEXT, callback);

        // then
        Throwable error = callback.getError();
        assertSame(exception, error);

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();    }

    // ========== New Tests for Defensive Improvements ==========

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should throw MongoSocketException when attempting to write to a closed stream")
    void shouldThrowExceptionWhenWritingToClosedStream() throws IOException {
        // given - open and then close stream
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);
        stream.close();

        // when/then - write should fail with MongoSocketException
        List<ByteBuf> buffers = createTestBuffers("test");

        MongoSocketException exception = assertThrows(MongoSocketException.class,
                () -> stream.write(buffers, OPERATION_CONTEXT));

        // stream.write doesn't release the passed in buffers
        ResourceUtil.release(buffers);

        assertTrue(exception.getMessage().contains("Stream is closed"));

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should fail async write operation with clear error when stream is closed")
    void shouldFailAsyncWriteWhenStreamIsClosed() throws IOException, InterruptedException {
        // given - open and then close stream
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);
        stream.close();

        // when - attempt async write
        List<ByteBuf> buffers = createTestBuffers("test");

        CallbackErrorHolder<Void> callback = new CallbackErrorHolder<>();
        stream.writeAsync(buffers, OPERATION_CONTEXT, callback);

        // then
        Throwable error = callback.getError();
        assertNotNull(error);
        assertInstanceOf(MongoSocketException.class, error);
        assertTrue(error.getMessage().contains("Stream is closed"));

        // stream.writeAsync doesn't release the passed in buffers
        ResourceUtil.release(buffers);

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should throw exception when attempting to read from a closed stream")
    void shouldThrowExceptionWhenReadingFromClosedStream() throws IOException {
        // given - open and then close stream
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);
        stream.close();

        // when/then - read should fail with IOException or MongoSocketException
        Exception exception = assertThrows(Exception.class,
                () -> stream.read(1024, OPERATION_CONTEXT));

        assertTrue(exception instanceof IOException || exception instanceof MongoSocketException,
                "Expected IOException or MongoSocketException but got: " + exception.getClass().getName());
        assertTrue(exception.getMessage().contains("closed")
                || exception.getMessage().contains("Channel is not active")
                || exception.getMessage().contains("connection"));

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should handle multiple consecutive close() calls gracefully without errors")
    void shouldHandleMultipleCloseCallsGracefully() throws IOException {
        // given
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);

        // when - close multiple times
        stream.close();
        stream.close();
        stream.close();

        // then - should not throw exception and stream should be closed
        assertTrue(stream.isClosed());

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should prevent write operations after the underlying channel becomes inactive")
    void shouldPreventWriteOperationsAfterChannelBecomesInactive() throws IOException {
        // given - stream opened
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);

        // Simulate channel becoming inactive by closing
        stream.close();

        // when/then - operations should fail
        List<ByteBuf> buffers = createTestBuffers("test");

        MongoSocketException exception = assertThrows(MongoSocketException.class,
                () -> stream.write(buffers, OPERATION_CONTEXT));

        // stream.write doesn't release the passed in buffers
        ResourceUtil.release(buffers);

        assertTrue(exception.getMessage().contains("closed")
                || exception.getMessage().contains("not active"));

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should validate connection state before executing write operations")
    void shouldValidateConnectionStateBeforeWrite() throws IOException {
        // given - create stream but don't open it
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());

        // when/then - write should fail because stream not opened
        List<ByteBuf> buffers = createTestBuffers("test");

        assertThrows(MongoSocketException.class,
                () -> stream.write(buffers, OPERATION_CONTEXT));

        // stream.write doesn't release the passed in buffers
        ResourceUtil.release(buffers);

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should handle async write failure gracefully with proper error callback")
    void shouldHandleAsyncWriteFailureGracefully() throws InterruptedException {
        // given - stream not opened
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());

        // when - attempt async write without opening
        List<ByteBuf> buffers = createTestBuffers("test");

        CallbackErrorHolder<Void> callback = new CallbackErrorHolder<>();
        stream.writeAsync(buffers, OPERATION_CONTEXT, callback);

        // then - should fail with clear error
        Throwable error = callback.getError();
        assertNotNull(error);
        assertInstanceOf(MongoSocketException.class, error);

        // stream.writeAsync doesn't release the passed in buffers
        ResourceUtil.release(buffers);

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisplayName("Should return the correct server address from getAddress()")
    void shouldReturnCorrectAddress() {
        // given
        ServerAddress serverAddress = new ServerAddress("localhost", 27017);
        factory = createDefaultFactory();
        stream = factory.create(serverAddress);

        // when
        ServerAddress address = stream.getAddress();

        // then
        assertEquals(serverAddress, address);

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisplayName("Should correctly report stream closed state throughout its lifecycle")
    void shouldReportClosedStateCorrectly() throws IOException {
        // given
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());

        // when/then - initially not closed (even without opening)
        assertFalse(stream.isClosed());

        // open and verify still not closed
        stream.open(OPERATION_CONTEXT);
        assertFalse(stream.isClosed());

        // close and verify is closed
        stream.close();
        assertTrue(stream.isClosed());

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should handle concurrent close() operations from multiple threads without errors")
    void shouldHandleConcurrentCloseGracefully() throws IOException, InterruptedException {
        // given
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);

        // when - close from multiple threads
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(3);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        for (int i = 0; i < 3; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    stream.close();
                } catch (Exception e) {
                    exceptionRef.set(e);
                } finally {
                    completionLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(completionLatch.await(5, TimeUnit.SECONDS));

        // then - no exceptions should occur
        assertNull(exceptionRef.get());
        assertTrue(stream.isClosed());

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should prevent ByteBuf leaks when write operations fail on a closed stream")
    void shouldPreventBufferLeakWhenWriteFailsOnClosedStream() throws IOException, InterruptedException {
        // given - create and open stream
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);

        // Close the stream
        stream.close();

        // when - attempt multiple writes (to test buffer cleanup)
        List<ByteBuf> buffers = createTestBuffers("test1", "test2");

        CallbackErrorHolder<Void> callback1 = new CallbackErrorHolder<>();
        CallbackErrorHolder<Void> callback2 = new CallbackErrorHolder<>();
        CallbackErrorHolder<Void> callback3 = new CallbackErrorHolder<>();

        stream.writeAsync(buffers, OPERATION_CONTEXT, callback1);
        stream.writeAsync(buffers, OPERATION_CONTEXT, callback2);
        stream.writeAsync(buffers, OPERATION_CONTEXT, callback3);

        // then - all should fail gracefully
        assertNotNull(callback1.getError());
        assertNotNull(callback2.getError());
        assertNotNull(callback3.getError());

        // stream.writeAsync doesn't release the passed in buffers
        ResourceUtil.release(buffers);

        // Verify no buffer leaks - stream should have released buffers on failed async write
        trackingAllocator.assertAllBuffersReleased();
    }

    // ========== Additional Comprehensive Test Scenarios ==========

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should not allocate composite buffer when channel is inactive before write")
    void shouldNotAllocateBufferWhenChannelInactiveBeforeWrite() throws IOException, InterruptedException {
        // given - create stream but immediately close it
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);
        stream.close();

        // when - attempt async write with large buffers (would allocate resources)
        List<ByteBuf> largeBuffers = createLargeTestBuffers(1024 * 1024, 2);

        CallbackErrorHolder<Void> callback = new CallbackErrorHolder<>();
        stream.writeAsync(largeBuffers, OPERATION_CONTEXT, callback);

        // then - should fail immediately without allocating composite buffer
        Throwable error = callback.getError();
        assertNotNull(error);
        assertTrue(error.getMessage().contains("closed") || error.getMessage().contains("not active"));

        // stream.writeAsync doesn't release the passed in buffers
        ResourceUtil.release(largeBuffers);

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should fail fast when attempting operations on never-opened stream")
    void shouldFailFastOnNeverOpenedStream() {
        // given - stream created but never opened
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());

        // when/then - all operations should fail immediately
        List<ByteBuf> buffers = createTestBuffers("test");

        // Write should fail
        assertThrows(MongoSocketException.class,
                () -> stream.write(buffers, OPERATION_CONTEXT));

        // stream.write doesn't release the passed in buffers
        ResourceUtil.release(buffers);

        // Read should fail
        assertThrows(Exception.class,
                () -> stream.read(1024, OPERATION_CONTEXT));

        // Stream should not be marked as closed (never opened)
        assertFalse(stream.isClosed());

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should handle async read operation gracefully on closed stream")
    void shouldHandleAsyncReadOnClosedStream() throws IOException, InterruptedException {
        // given - open and close stream
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);
        stream.close();

        // when - attempt async read
        CallbackErrorHolder<org.bson.ByteBuf> callback = new CallbackErrorHolder<>();
        stream.readAsync(1024, OPERATION_CONTEXT, callback);

        // then - should fail with appropriate error
        Throwable error = callback.getError();
        assertNotNull(error);
        assertTrue(error instanceof IOException || error instanceof MongoSocketException);

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @SuppressWarnings("WriteOnlyObject")
    @DisplayName("Should handle concurrent write and close operations without deadlock")
    void shouldHandleConcurrentWriteAndCloseWithoutDeadlock() throws IOException, InterruptedException {
        // given
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);

        CountDownLatch writeLatch = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);
        AtomicReference<Exception> writeException = new AtomicReference<>();
        AtomicReference<Exception> closeException = new AtomicReference<>();

        // when - write and close concurrently
        Thread writeThread = new Thread(() -> {
            List<ByteBuf> buffers = createTestBuffers("concurrent test");
            try {
                writeLatch.await();
                stream.write(buffers, OPERATION_CONTEXT);
            } catch (Exception e) {
                writeException.set(e);
            } finally {
                // stream.write doesn't release the passed in buffers
                ResourceUtil.release(buffers);
            }
        });

        Thread closeThread = new Thread(() -> {
            try {
                closeLatch.await();
                stream.close();
            } catch (Exception e) {
                closeException.set(e);
            }
        });

        writeThread.start();
        closeThread.start();

        // Start both operations nearly simultaneously
        writeLatch.countDown();
        closeLatch.countDown();

        // Wait for completion
        writeThread.join(5000);
        closeThread.join(5000);

        // then - no deadlock should occur, operations complete
        assertFalse(writeThread.isAlive(), "Write thread should complete");
        assertFalse(closeThread.isAlive(), "Close thread should complete");
        assertTrue(stream.isClosed(), "Stream should be closed");

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should handle empty buffer list in write operations")
    void shouldHandleEmptyBufferList() throws IOException {
        // given
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);

        // when - write empty buffer list
        List<ByteBuf> emptyBuffers = emptyList();

        // then - should not throw exception (implementation specific behavior)
        assertDoesNotThrow(() -> stream.write(emptyBuffers, OPERATION_CONTEXT));

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @Tag("Slow")
    @DisabledIf("isSslEnabled")
    @DisplayName("Should properly cleanup resources when async open operation fails")
    void shouldCleanupResourcesWhenAsyncOpenFails() throws InterruptedException {
        // given - factory with unreachable address
        SocketSettings socketSettings = SocketSettings.builder()
                .connectTimeout(500, TimeUnit.MILLISECONDS) // Longer timeout to allow connection attempt
                .build();
        SslSettings sslSettings = SslSettings.builder().build();

        InetAddressResolver resolver = host -> {
            try {
                return singletonList(InetAddress.getByName("192.168.255.255")); // Unreachable
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        };

        factory = new NettyStreamFactory(resolver, socketSettings, sslSettings,
                eventLoopGroup, NioSocketChannel.class, trackingAllocator, null);
        stream = factory.create(new ServerAddress());

        // when - attempt async open
        CallbackErrorHolder<Void> callback = new CallbackErrorHolder<>();
        stream.openAsync(OPERATION_CONTEXT, callback);

        // then - should fail and cleanup resources (may take time due to connection timeout)
        Throwable error = callback.getError();
        assertNotNull(error);
        assertInstanceOf(MongoSocketException.class, error);

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisplayName("Should return consistent ServerAddress across multiple calls")
    void shouldReturnConsistentServerAddress() {
        // given
        ServerAddress serverAddress = new ServerAddress("test.mongodb.com", 27017);
        factory = createDefaultFactory();
        stream = factory.create(serverAddress);

        // when - call getAddress multiple times
        ServerAddress addr1 = stream.getAddress();
        ServerAddress addr2 = stream.getAddress();
        ServerAddress addr3 = stream.getAddress();

        // then - all should return same address
        assertEquals(serverAddress, addr1);
        assertEquals(serverAddress, addr2);
        assertEquals(serverAddress, addr3);
        assertSame(addr1, addr2); // Should return same instance

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should handle rapid open and close cycles without resource leaks")
    void shouldHandleRapidOpenCloseCycles() throws IOException {
        // given
        factory = createDefaultFactory();

        // when - perform multiple rapid open/close cycles
        for (int i = 0; i < 5; i++) {
            stream = factory.create(new ServerAddress());
            stream.open(OPERATION_CONTEXT);
            assertFalse(stream.isClosed(), "Stream should be open after opening");

            stream.close();
            assertTrue(stream.isClosed(), "Stream should be closed after closing");
        }

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should validate stream state remains consistent after failed operations")
    void shouldMaintainConsistentStateAfterFailedOperations() throws IOException {
        // given
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);
        ServerAddress originalAddress = stream.getAddress();

        // when - perform operation that will fail
        stream.close();

        List<ByteBuf> buffers = createTestBuffers("test");

        try {
            stream.write(buffers, OPERATION_CONTEXT);
        } catch (MongoSocketException e) {
            // Expected
        } finally  {
            // stream.write doesn't release the passed in buffers
            ResourceUtil.release(buffers);
        }

        // then - stream state should remain consistent
        assertTrue(stream.isClosed());
        assertEquals(originalAddress, stream.getAddress());

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should release retained buffers when stream closes during read assembly")
    void shouldReleaseBuffersWhenStreamClosesDuringReadAssembly() throws IOException, InterruptedException {
        // This test simulates the race condition where:
        // 1. Async read is initiated (creates pendingReader)
        // 2. Stream is closed by another thread before data arrives
        // 3. If data were to arrive and be retained, it must be cleaned up
        // 4. The fix ensures readAsync() releases buffers when detecting closed state

        // given - open stream and initiate async read
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);

        // Start an async read that will wait for data (creates pendingReader)
        CallbackErrorHolder<org.bson.ByteBuf> asyncReadCallback = new CallbackErrorHolder<>();
        stream.readAsync(1024, OPERATION_CONTEXT, asyncReadCallback);

        // Give the async read time to set up
        Thread.sleep(10);

        // when - close the stream while async read is pending
        stream.close();

        // then - the async read should fail with appropriate error
        Throwable error = asyncReadCallback.getError();
        assertNotNull(error, "Async read should fail when stream is closed");
        assertTrue(error instanceof IOException || error instanceof MongoSocketException,
                "Expected IOException or MongoSocketException but got: " + error.getClass().getName());
        assertTrue(stream.isClosed(), "Stream should be closed");

        // Attempt another read after close - should also fail and clean up any pending buffers
        Exception exception = assertThrows(Exception.class,
                () -> stream.read(1024, OPERATION_CONTEXT));
        assertTrue(exception instanceof IOException || exception instanceof MongoSocketException);

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @DisplayName("Should handle rapid close during async read operation with race condition")
    void shouldHandleCloseRaceWithConcurrentReads() throws IOException, InterruptedException {
        // This test exercises the specific race condition from the SSL/TLS leak:
        // Thread 1: Initiates async read (creates pendingReader)
        // Thread 2: Closes the stream
        // Thread 1: If data arrives, handleReadResponse would retain buffer
        // Thread 1: readAsync detects closed state and must clean up

        // given
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);

        // Start an async read that will wait for data
        CallbackErrorHolder<org.bson.ByteBuf> callback = new CallbackErrorHolder<>();

        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicReference<Exception> closeException = new AtomicReference<>();

        // Thread 1: Start async read
        Thread readThread = new Thread(() -> {
            try {
                readStarted.countDown();
                stream.readAsync(1024, OPERATION_CONTEXT, callback);
                closeStarted.await(); // Wait for close to happen
            } catch (Exception e) {
                // Expected - stream may be closed
            }
        });

        // Thread 2: Close the stream
        Thread closeThread = new Thread(() -> {
            try {
                readStarted.await(); // Wait for read to start
                Thread.sleep(10); // Small delay to increase race window
                closeStarted.countDown();
                stream.close();
            } catch (Exception e) {
                closeException.set(e);
            }
        });

        // when - execute both threads
        readThread.start();
        closeThread.start();

        readThread.join(2000);
        closeThread.join(2000);

        // then - threads should complete
        assertFalse(readThread.isAlive(), "Read thread should complete");
        assertFalse(closeThread.isAlive(), "Close thread should complete");
        assertNull(closeException.get(), "Close should not throw exception");
        assertTrue(stream.isClosed(), "Stream should be closed");

        // The async read callback should either:
        // - Receive an error (if it noticed the close), OR
        // - Still be waiting (if close happened first and prevented read setup)
        // Either way, no buffer leaks should occur

        // Try another operation to ensure state is consistent
        assertThrows(Exception.class, () -> stream.read(100, OPERATION_CONTEXT),
                "Operations after close should fail");

        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    @Test
    @DisabledIf("isSslEnabled")
    @SuppressWarnings("ThrowableNotThrown")
    @DisplayName("Should handle multiple async operations with different callbacks")
    void shouldHandleMultipleAsyncOperationsWithDifferentCallbacks() throws IOException, InterruptedException {
        // given
        factory = createDefaultFactory();
        stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);

        List<ByteBuf> buffers = createTestBuffers("async1");

        // when - submit multiple async writes with different callbacks
        CallbackErrorHolder<Void> callback1 = new CallbackErrorHolder<>();
        CallbackErrorHolder<Void> callback2 = new CallbackErrorHolder<>();
        CallbackErrorHolder<Void> callback3 = new CallbackErrorHolder<>();

        stream.writeAsync(buffers, OPERATION_CONTEXT, callback1);
        stream.writeAsync(buffers, OPERATION_CONTEXT, callback2);
        stream.writeAsync(buffers, OPERATION_CONTEXT, callback3);

        // then - all callbacks should be invoked (either success or failure)
        // Wait for all to complete
        callback1.getError(); // May be null (success) or error
        callback2.getError();
        callback3.getError();

        // stream.writeAsync doesn't release the passed in buffers
        ResourceUtil.release(buffers);

        // Test passes if we reach here without timeout
        // Verify no buffer leaks
        trackingAllocator.assertAllBuffersReleased();
    }

    // ========== Helper Methods ==========

    /**
     * Creates ByteBufs with the given data, wrapped in NettyByteBuf.
     */
    private List<ByteBuf> createTestBuffers(final String... dataArray) {
        List<ByteBuf> buffers = new ArrayList<>();
        for (String data : dataArray) {
            io.netty.buffer.ByteBuf nettyBuffer = trackingAllocator.buffer();
            nettyBuffer.writeBytes(data.getBytes());
            buffers.add(new NettyByteBuf(nettyBuffer));
        }
        return buffers;
    }

    /**
     * Creates multiple large ByteBufs with specified capacity, wrapped in NettyByteBuf.
     */
    private List<ByteBuf> createLargeTestBuffers(final int capacityBytes, final int count) {
        List<ByteBuf> buffers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            io.netty.buffer.ByteBuf nettyBuffer = trackingAllocator.buffer(capacityBytes);
            buffers.add(new NettyByteBuf(nettyBuffer));
        }
        return buffers;
    }

    private NettyStreamFactory createDefaultFactory() {
        SocketSettings socketSettings = SocketSettings.builder()
                .connectTimeout(10000, TimeUnit.MILLISECONDS)
                .build();
        SslSettings sslSettings = SslSettings.builder().build();

        InetAddressResolver resolver = host -> {
            try {
                return singletonList(InetAddress.getByName(host));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        };

        // Use tracking allocator for all tests to get explicit leak verification
        return new NettyStreamFactory(resolver, socketSettings, sslSettings,
                eventLoopGroup, NioSocketChannel.class, trackingAllocator, null);
    }

    /**
     * Helper class to capture async completion handler results.
     */
    private static class CallbackErrorHolder<T> implements AsyncCompletionHandler<T> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        private final AtomicReference<T> resultRef = new AtomicReference<>();

        Throwable getError() throws InterruptedException {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "Callback not completed within timeout");
            return throwableRef.get();
        }

        T getResult() throws InterruptedException {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "Callback not completed within timeout");
            return resultRef.get();
        }

        @Override
        public void completed(final T result) {
            resultRef.set(result);
            latch.countDown();
        }

        @Override
        public void failed(@NotNull final Throwable t) {
            throwableRef.set(t);
            latch.countDown();
        }
    }

    /**
     * Tracking allocator that records all buffer allocations and can verify they are all released.
     * This allows us to explicitly prove no buffer leaks occur, rather than relying solely on
     * Netty's leak detector.
     */
    private static class TrackingNettyByteBufAllocator extends PooledByteBufAllocator {
        private final List<io.netty.buffer.ByteBuf> allocatedBuffers = new ArrayList<>();
        private final AtomicInteger allocationCount = new AtomicInteger(0);

        TrackingNettyByteBufAllocator() {
            super(false); // Use heap buffers for testing
        }

        @Override
        public io.netty.buffer.CompositeByteBuf compositeBuffer(final int maxNumComponents) {
            io.netty.buffer.CompositeByteBuf buffer = super.compositeBuffer(maxNumComponents);
            trackBuffer(buffer);
            return buffer;
        }

        @Override
        public io.netty.buffer.CompositeByteBuf compositeDirectBuffer(final int maxNumComponents) {
            io.netty.buffer.CompositeByteBuf buffer = super.compositeDirectBuffer(maxNumComponents);
            trackBuffer(buffer);
            return buffer;
        }

        @Override
        public io.netty.buffer.ByteBuf buffer() {
            io.netty.buffer.ByteBuf buffer = super.buffer();
            trackBuffer(buffer);
            return buffer;
        }

        @Override
        public io.netty.buffer.ByteBuf buffer(final int initialCapacity) {
            io.netty.buffer.ByteBuf buffer = super.buffer(initialCapacity);
            trackBuffer(buffer);
            return buffer;
        }

        @Override
        public io.netty.buffer.ByteBuf buffer(final int initialCapacity, final int maxCapacity) {
            io.netty.buffer.ByteBuf buffer = super.buffer(initialCapacity, maxCapacity);
            trackBuffer(buffer);
            return buffer;
        }

        @Override
        public io.netty.buffer.ByteBuf directBuffer(final int initialCapacity, final int maxCapacity) {
            io.netty.buffer.ByteBuf buffer = super.directBuffer(initialCapacity, maxCapacity);
            trackBuffer(buffer);
            return buffer;
        }

        private void trackBuffer(final io.netty.buffer.ByteBuf buffer) {
            synchronized (allocatedBuffers) {
                allocatedBuffers.add(buffer);
                allocationCount.incrementAndGet();
            }
        }

        /**
         * Asserts that all allocated buffers have been released (refCnt == 0).
         * This provides explicit proof that no leaks occurred.
         */
        void assertAllBuffersReleased() {
            synchronized (allocatedBuffers) {
                List<io.netty.buffer.ByteBuf> leakedBuffers = new ArrayList<>();
                for (io.netty.buffer.ByteBuf buffer : allocatedBuffers) {
                    if (buffer.refCnt() > 0) {
                        leakedBuffers.add(buffer);
                    }
                }

                if (!leakedBuffers.isEmpty()) {
                    StringBuilder message = new StringBuilder();
                    message.append("BUFFER LEAK DETECTED: ")
                            .append(leakedBuffers.size())
                            .append(" of ")
                            .append(allocatedBuffers.size())
                            .append(" buffers were not released\n");

                    message.append(getStats())
                            .append("\n");

                    for (int i = 0; i < leakedBuffers.size(); i++) {
                        io.netty.buffer.ByteBuf leaked = leakedBuffers.get(i);
                        message.append("  [").append(i).append("] ")
                                .append(leaked.getClass().getSimpleName())
                                .append(" refCnt=").append(leaked.refCnt())
                                .append(" capacity=").append(leaked.capacity())
                                .append("\n");
                    }

                    leakedBuffers.forEach(ReferenceCounted::release);
                    fail(message.toString());
                }
            }
        }

        /**
         * Returns allocation statistics for debugging.
         */
        String getStats() {
            synchronized (allocatedBuffers) {
                int leaked = 0;
                int released = 0;
                for (io.netty.buffer.ByteBuf buffer : allocatedBuffers) {
                    if (buffer.refCnt() > 0) {
                        leaked++;
                    } else {
                        released++;
                    }
                }
                return String.format("Allocations: %d, Released: %d, Leaked: %d",
                        allocationCount.get(), released, leaked);
            }
        }
    }
}
