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

package com.mongodb.internal.connection.netty;

import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.bson.ByteBuf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the listener that {@code OpenChannelFutureListener} registers on {@code channel.closeFuture()},
 * from both angles:
 * <ul>
 *     <li>behavior: a read waiting on the channel must be failed when the channel closes;</li>
 *     <li>footprint: the listener must not keep the connection-open {@link AsyncCompletionHandler}
 *     (and everything it transitively references, e.g. the callback chain of the operation that was
 *     waiting for the connection) strongly reachable while the channel remains open.</li>
 * </ul>
 */
public class NettyStreamCloseFutureListenerTest {

    private ServerSocket serverSocket;
    private NioEventLoopGroup eventLoopGroup;
    private NettyStream stream;

    @BeforeEach
    public void setUp() throws Exception {
        serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        eventLoopGroup = new NioEventLoopGroup();
        stream = (NettyStream) new NettyStreamFactory(
                host -> Collections.singletonList(InetAddress.getLoopbackAddress()),
                SocketSettings.builder().connectTimeout(10, TimeUnit.SECONDS).build(),
                SslSettings.builder().build(),
                eventLoopGroup, NioSocketChannel.class, PooledByteBufAllocator.DEFAULT, null)
                .create(new ServerAddress("127.0.0.1", serverSocket.getLocalPort()));
    }

    @AfterEach
    public void tearDown() throws Exception {
        stream.close();
        eventLoopGroup.shutdownGracefully().syncUninterruptibly();
        serverSocket.close();
    }

    @Test
    @DisplayName("open handler should not remain strongly reachable from the open channel after the open completes")
    public void shouldReleaseOpenHandlerAfterOpenCompletesWhileChannelRemainsOpen() throws Exception {
        // Open a connection with a handler, and keep only a WeakReference to that handler
        CountDownLatch opened = new CountDownLatch(1);
        AsyncCompletionHandler<Void> handler = new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void result) {
                opened.countDown();
            }

            @Override
            public void failed(final Throwable t) {
                opened.countDown();
            }
        };
        WeakReference<AsyncCompletionHandler<Void>> canary = new WeakReference<>(handler);

        stream.openAsync(OPERATION_CONTEXT, handler);
        assertTrue(opened.await(10, TimeUnit.SECONDS), "open did not complete");

        // Nullify the test's own reference so the driver is the only thing that could still retain the handler.
        // Note: the channel is deliberately left open (never closed) for the rest of the test, so a positive
        // result proves the handler is released while the channel is alive.
        handler = null;

        // The channel's closeFuture listener must not keep the one-shot open handler alive,
        // so with no strong reference remaining the handler must be garbage-collectable.
        assertRefUnreachable(canary,
                "the connection-open AsyncCompletionHandler must not stay strongly reachable from the open "
                        + "channel (closeFuture listener) after the open has completed");
    }

    @Test
    @DisplayName("pending read should be failed when the channel is closed")
    public void shouldFailPendingReadWhenChannelIsClosed() throws Exception {
        Socket acceptedSocket = openAndAcceptConnection();

        // Create a read that 127.0.0.1 will never satisfy, then close the connection from the server side
        CountDownLatch readCompleted = new CountDownLatch(1);
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        stream.readAsync(4, OPERATION_CONTEXT, new AsyncCompletionHandler<ByteBuf>() {
            @Override
            public void completed(final ByteBuf result) {
                readCompleted.countDown();
            }

            @Override
            public void failed(final Throwable t) {
                readFailure.set(t);
                readCompleted.countDown();
            }
        });

        acceptedSocket.close();

        // Assert the closeFuture listener fires and fails the pending read with the expected IOException
        assertTrue(readCompleted.await(10, TimeUnit.SECONDS), "the pending read never completed");
        Throwable failure = readFailure.get();
        assertNotNull(failure, "the pending read completed successfully instead of failing");
        assertInstanceOf(IOException.class, failure);
        assertEquals("The connection to the server was closed", failure.getMessage());
    }

    /**
     * Asserts that the referent of {@code ref} is no longer strongly reachable, i.e. no strong reference to it
     * remains and it is therefore garbage-collectable.
     * A {@link WeakReference} is a reachability probe: the garbage collector clears it as soon as its referent is
     * no longer strongly reachable. {@link System#gc()} is only a hint, so we nudge it a few times and give the
     * collector a moment before checking - if the referent is genuinely unreachable the reference clears quickly.
     */
    @SuppressWarnings("BusyWait")
    private static void assertRefUnreachable(final WeakReference<?> ref, final String message) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (ref.get() != null && System.nanoTime() < deadlineNanos) {
            System.gc();
            Thread.sleep(100);
        }
        assertNull(ref.get(), message);
    }

    /**
     * Opens {@link #stream} against the local {@link #serverSocket} and returns the server side of the accepted
     * connection, so the test can later close it to simulate the server dropping the connection.
     */
    private Socket openAndAcceptConnection() throws Exception {
        AtomicReference<Socket> acceptedSocket = new AtomicReference<>();
        Thread acceptor = new Thread(() -> {
            try {
                acceptedSocket.set(serverSocket.accept());
            } catch (IOException ignored) {
                // the assertion below fails if nothing was accepted
            }
        });
        acceptor.start();

        stream.open(OPERATION_CONTEXT);
        acceptor.join(TimeUnit.SECONDS.toMillis(10));
        assertNotNull(acceptedSocket.get(), "the server never accepted the connection");
        return acceptedSocket.get();
    }
}
