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

package com.mongodb.connection;

import com.mongodb.MongoClientException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.internal.connection.AsynchronousChannelStream;
import com.mongodb.internal.connection.ExtendedAsynchronousByteChannel;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.internal.connection.tlschannel.BufferAllocator;
import com.mongodb.internal.connection.tlschannel.ClientTlsChannel;
import com.mongodb.internal.connection.tlschannel.TlsChannel;
import com.mongodb.internal.connection.tlschannel.async.AsynchronousTlsChannel;
import com.mongodb.internal.connection.tlschannel.async.AsynchronousTlsChannelGroup;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.io.Closeable;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.internal.connection.SslHelper.enableHostNameVerification;
import static com.mongodb.internal.connection.SslHelper.enableSni;
import static java.util.Optional.ofNullable;

/**
 * A {@code StreamFactoryFactory} that supports TLS/SSL.  The implementation supports asynchronous usage.
 *
 * @since 3.10
 */
public class TlsChannelStreamFactoryFactory implements StreamFactoryFactory, Closeable {

    private static final Logger LOGGER = Loggers.getLogger("connection.tls");

    private final SelectorMonitor selectorMonitor;
    private final AsynchronousTlsChannelGroup group;
    private final boolean ownsGroup;
    private final PowerOfTwoBufferPool bufferPool = PowerOfTwoBufferPool.DEFAULT;

    /**
     * Construct a new instance
     */
    public TlsChannelStreamFactoryFactory() {
        this(new AsynchronousTlsChannelGroup(), true);
    }

    /**
     * Construct a new instance with the given {@code AsynchronousTlsChannelGroup}.  Callers are required to close the provided group
     * in order to free up resources.
     *
     * @param group the group
     * @deprecated Prefer {@link #TlsChannelStreamFactoryFactory()}
     */
    @Deprecated
    public TlsChannelStreamFactoryFactory(final AsynchronousTlsChannelGroup group) {
        this(group, false);
    }

    private TlsChannelStreamFactoryFactory(final AsynchronousTlsChannelGroup group, final boolean ownsGroup) {
        this.group = group;
        this.ownsGroup = ownsGroup;
        selectorMonitor = new SelectorMonitor();
        selectorMonitor.start();
    }

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        return serverAddress -> new TlsChannelStream(serverAddress, socketSettings, sslSettings, bufferPool, group, selectorMonitor);
    }

    @Override
    public void close() {
        selectorMonitor.close();
        if (ownsGroup) {
            group.shutdown();
        }
    }

    private static class SelectorMonitor implements Closeable {

        private static final class Pair {
            private final SocketChannel socketChannel;
            private final Runnable attachment;

            private Pair(final SocketChannel socketChannel, final Runnable attachment) {
                this.socketChannel = socketChannel;
                this.attachment = attachment;
            }
        }

        private final Selector selector;
        private volatile boolean isClosed;
        private final ConcurrentLinkedDeque<Pair> pendingRegistrations = new ConcurrentLinkedDeque<>();

        SelectorMonitor() {
            try {
                this.selector = Selector.open();
            } catch (IOException e) {
                throw new MongoClientException("Exception opening Selector", e);
            }
        }

        void start() {
            Thread selectorThread = new Thread(() -> {
                try {
                    while (!isClosed) {
                        try {
                            selector.select();

                            for (SelectionKey selectionKey : selector.selectedKeys()) {
                                selectionKey.cancel();
                                Runnable runnable = (Runnable) selectionKey.attachment();
                                runnable.run();
                            }

                            for (Iterator<Pair> iter = pendingRegistrations.iterator(); iter.hasNext();) {
                                Pair pendingRegistration = iter.next();
                                pendingRegistration.socketChannel.register(selector, SelectionKey.OP_CONNECT,
                                        pendingRegistration.attachment);
                                iter.remove();
                            }
                        } catch (Exception e) {
                            LOGGER.warn("Exception in selector loop", e);
                        }
                    }
                } finally {
                    try {
                        selector.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            });
            selectorThread.setDaemon(true);
            selectorThread.start();
        }

        void register(final SocketChannel channel, final Runnable attachment) {
            pendingRegistrations.add(new Pair(channel, attachment));
            selector.wakeup();
        }

        @Override
        public void close() {
            isClosed = true;
            selector.wakeup();
        }
    }

    private static class TlsChannelStream extends AsynchronousChannelStream {

        private final AsynchronousTlsChannelGroup group;
        private final SelectorMonitor selectorMonitor;
        private final SslSettings sslSettings;

        TlsChannelStream(final ServerAddress serverAddress, final SocketSettings settings, final SslSettings sslSettings,
                         final PowerOfTwoBufferPool bufferProvider, final AsynchronousTlsChannelGroup group,
                         final SelectorMonitor selectorMonitor) {
            super(serverAddress, settings, bufferProvider);
            this.sslSettings = sslSettings;
            this.group = group;
            this.selectorMonitor = selectorMonitor;
        }

        @Override
        public boolean supportsAdditionalTimeout() {
            return true;
        }

        @Override
        public void openAsync(final AsyncCompletionHandler<Void> handler) {
            isTrue("unopened", getChannel() == null);
            try {
                SocketChannel socketChannel = SocketChannel.open();
                socketChannel.configureBlocking(false);

                socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                if (getSettings().getReceiveBufferSize() > 0) {
                    socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, getSettings().getReceiveBufferSize());
                }
                if (getSettings().getSendBufferSize() > 0) {
                    socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, getSettings().getSendBufferSize());
                }

                socketChannel.connect(getServerAddress().getSocketAddress());

                selectorMonitor.register(socketChannel, () -> {
                    try {
                        if (!socketChannel.finishConnect()) {
                            throw new MongoSocketOpenException("Failed to finish connect", getServerAddress());
                        }

                        SSLEngine sslEngine = getSslContext().createSSLEngine(getServerAddress().getHost(),
                                getServerAddress().getPort());
                        sslEngine.setUseClientMode(true);

                        SSLParameters sslParameters = sslEngine.getSSLParameters();
                        enableSni(getServerAddress().getHost(), sslParameters);

                        if (!sslSettings.isInvalidHostNameAllowed()) {
                            enableHostNameVerification(sslParameters);
                        }
                        sslEngine.setSSLParameters(sslParameters);

                        BufferAllocator bufferAllocator = new BufferProviderAllocator();

                        TlsChannel tlsChannel = ClientTlsChannel.newBuilder(socketChannel, sslEngine)
                                .withEncryptedBufferAllocator(bufferAllocator)
                                .withPlainBufferAllocator(bufferAllocator)
                                .build();

                        // build asynchronous channel, based in the TLS channel and associated with the global group.
                        setChannel(new AsynchronousTlsChannelAdapter(new AsynchronousTlsChannel(group, tlsChannel, socketChannel)));

                        handler.completed(null);
                    } catch (IOException e) {
                        handler.failed(new MongoSocketOpenException("Exception opening socket", getServerAddress(), e));
                    } catch (Throwable t) {
                        handler.failed(t);
                    }
                });
            } catch (IOException e) {
                handler.failed(new MongoSocketOpenException("Exception opening socket", getServerAddress(), e));
            } catch (Throwable t) {
                handler.failed(t);
            }
        }

        private SSLContext getSslContext() {
            try {
                return ofNullable(sslSettings.getContext()).orElse(SSLContext.getDefault());
            } catch (NoSuchAlgorithmException e) {
                throw new MongoClientException("Unable to create default SSLContext", e);
            }
        }

        private class BufferProviderAllocator implements BufferAllocator {
            @Override
            public ByteBuffer allocate(final int size) {
                return getBufferProvider().getByteBuffer(size);
            }

            @Override
            public void free(final ByteBuffer buffer) {
                getBufferProvider().release(buffer);
            }
        }

        public static class AsynchronousTlsChannelAdapter implements ExtendedAsynchronousByteChannel {
            private final AsynchronousTlsChannel wrapped;

            AsynchronousTlsChannelAdapter(final AsynchronousTlsChannel wrapped) {
                this.wrapped = wrapped;
            }

            @Override
            public <A> void read(final ByteBuffer dst, final A attach, final CompletionHandler<Integer, ? super A> handler) {
                wrapped.read(dst, attach, handler);
            }

            @Override
            public <A> void read(final ByteBuffer dst, final long timeout, final TimeUnit unit, @Nullable final A attach,
                                 final CompletionHandler<Integer, ? super A> handler) {
                wrapped.read(dst, timeout, unit, attach, handler);
            }

            @Override
            public <A> void read(final ByteBuffer[] dsts, final int offset, final int length, final long timeout, final TimeUnit unit,
                                 @Nullable final A attach, final CompletionHandler<Long, ? super A> handler) {
                wrapped.read(dsts, offset, length, timeout, unit, attach, handler);
            }

            @Override
            public Future<Integer> read(final ByteBuffer dst) {
                return wrapped.read(dst);
            }

            @Override
            public <A> void write(final ByteBuffer src, final A attach, final CompletionHandler<Integer, ? super A> handler) {
                wrapped.write(src, attach, handler);
            }

            @Override
            public <A> void write(final ByteBuffer src, final long timeout, final TimeUnit unit, final A attach,
                                  final CompletionHandler<Integer, ? super A> handler) {
                wrapped.write(src, timeout, unit, attach, handler);
            }

            @Override
            public <A> void write(final ByteBuffer[] srcs, final int offset, final int length, final long timeout, final TimeUnit unit,
                                  final A attach, final CompletionHandler<Long, ? super A> handler) {
                wrapped.write(srcs, offset, length, timeout, unit, attach, handler);
            }

            @Override
            public Future<Integer> write(final ByteBuffer src) {
                return wrapped.write(src);
            }

            @Override
            public boolean isOpen() {
                return wrapped.isOpen();
            }

            @Override
            public void close() throws IOException {
                wrapped.close();
            }
        }
    }
}

