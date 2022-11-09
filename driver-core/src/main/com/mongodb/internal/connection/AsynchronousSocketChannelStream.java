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

import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.isTrue;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class AsynchronousSocketChannelStream extends AsynchronousChannelStream {
    private final ServerAddress serverAddress;
    private final SocketSettings settings;
    private final AsynchronousChannelGroup group;

    public AsynchronousSocketChannelStream(final ServerAddress serverAddress, final SocketSettings settings,
                                          final PowerOfTwoBufferPool bufferProvider, final AsynchronousChannelGroup group) {
        super(serverAddress, settings, bufferProvider);
        this.serverAddress = serverAddress;
        this.settings = settings;
        this.group = group;
    }

    @Override
    public void openAsync(final AsyncCompletionHandler<Void> handler) {
        isTrue("unopened", getChannel() == null);
        Queue<SocketAddress> socketAddressQueue;

        try {
            socketAddressQueue = new LinkedList<>(serverAddress.getSocketAddresses());
        } catch (Throwable t) {
            handler.failed(t);
            return;
        }

        initializeSocketChannel(handler, socketAddressQueue);
    }

    private void initializeSocketChannel(final AsyncCompletionHandler<Void> handler, final Queue<SocketAddress> socketAddressQueue) {
        if (socketAddressQueue.isEmpty()) {
            handler.failed(new MongoSocketException("Exception opening socket", serverAddress));
        } else {
            SocketAddress socketAddress = socketAddressQueue.poll();

            try {
                AsynchronousSocketChannel attemptConnectionChannel = AsynchronousSocketChannel.open(group);
                attemptConnectionChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                attemptConnectionChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                if (settings.getReceiveBufferSize() > 0) {
                    attemptConnectionChannel.setOption(StandardSocketOptions.SO_RCVBUF, settings.getReceiveBufferSize());
                }
                if (settings.getSendBufferSize() > 0) {
                    attemptConnectionChannel.setOption(StandardSocketOptions.SO_SNDBUF, settings.getSendBufferSize());
                }

                attemptConnectionChannel.connect(socketAddress, null,
                        new OpenCompletionHandler(handler, socketAddressQueue, attemptConnectionChannel));
            } catch (IOException e) {
                handler.failed(new MongoSocketOpenException("Exception opening socket", serverAddress, e));
            } catch (Throwable t) {
                handler.failed(t);
            }
        }
    }

    public AsynchronousChannelGroup getGroup() {
        return group;
    }

    private class OpenCompletionHandler implements CompletionHandler<Void, Object>  {
        private final AtomicReference<AsyncCompletionHandler<Void>> handlerReference;
        private final Queue<SocketAddress> socketAddressQueue;
        private final AsynchronousSocketChannel attemptConnectionChannel;

        OpenCompletionHandler(final AsyncCompletionHandler<Void> handler, final Queue<SocketAddress> socketAddressQueue,
                              final AsynchronousSocketChannel attemptConnectionChannel) {
            this.handlerReference = new AtomicReference<>(handler);
            this.socketAddressQueue = socketAddressQueue;
            this.attemptConnectionChannel = attemptConnectionChannel;
        }

        @Override
        public void completed(final Void result, final Object attachment) {
            setChannel(new AsynchronousSocketChannelAdapter(attemptConnectionChannel));
            handlerReference.getAndSet(null).completed(null);
        }

        @Override
        public void failed(final Throwable exc, final Object attachment) {
            AsyncCompletionHandler<Void> localHandler = handlerReference.getAndSet(null);

            if (socketAddressQueue.isEmpty()) {
                if (exc instanceof IOException) {
                    localHandler.failed(new MongoSocketOpenException("Exception opening socket", getAddress(), exc));
                } else {
                    localHandler.failed(exc);
                }
            } else {
                initializeSocketChannel(localHandler, socketAddressQueue);
            }
        }
    }

    private static final class AsynchronousSocketChannelAdapter implements ExtendedAsynchronousByteChannel {
        private final AsynchronousSocketChannel channel;

        private AsynchronousSocketChannelAdapter(final AsynchronousSocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public <A> void read(final ByteBuffer dst, final long timeout, final TimeUnit unit, final A attach,
                             final CompletionHandler<Integer, ? super A> handler) {
            channel.read(dst, timeout, unit, attach, handler);
        }

        @Override
        public <A> void read(final ByteBuffer[] dsts, final int offset, final int length, final long timeout, final TimeUnit unit,
                             final A attach, final CompletionHandler<Long, ? super A> handler) {
            channel.read(dsts, offset, length, timeout, unit, attach, handler);
        }

        @Override
        public <A> void write(final ByteBuffer src, final long timeout, final TimeUnit unit, final A attach,
                              final CompletionHandler<Integer, ? super A> handler) {
            channel.write(src, timeout, unit, attach, handler);
        }

        @Override
        public <A> void write(final ByteBuffer[] srcs, final int offset, final int length, final long timeout, final TimeUnit unit,
                              final A attach, final CompletionHandler<Long, ? super A> handler) {
            channel.write(srcs, offset, length, timeout, unit, attach, handler);
        }

        @Override
        public <A> void read(final ByteBuffer dst, final A attachment, final CompletionHandler<Integer, ? super A> handler) {
            channel.read(dst, attachment, handler);
        }

        @Override
        public Future<Integer> read(final ByteBuffer dst) {
            return channel.read(dst);
        }

        @Override
        public <A> void write(final ByteBuffer src, final A attachment, final CompletionHandler<Integer, ? super A> handler) {
            channel.write(src, attachment, handler);
        }

        @Override
        public Future<Integer> write(final ByteBuffer src) {
            return channel.write(src);
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
