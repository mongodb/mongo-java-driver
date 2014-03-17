/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb.connection.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GenericFutureListener;
import org.bson.ByteBuf;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.connection.AsyncCompletionHandler;
import org.mongodb.connection.SSLSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SocketSettings;
import org.mongodb.connection.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * A Stream implementation based on Netty 4.0.
 */
final class NettyStream implements Stream {
    private final ServerAddress address;
    private SocketChannel socketChannel;
    private volatile boolean isClosed;

    private final LinkedList<io.netty.buffer.ByteBuf> pendingInboundBuffers = new LinkedList<io.netty.buffer.ByteBuf>();
    private PendingReader pendingReader;
    private Throwable pendingException;

    public NettyStream(final ServerAddress address, final SocketSettings settings, final SSLSettings sslSettings,
                       final EventLoopGroup workerGroup) {
        this.address = address;
        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, settings.isKeepAlive());
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(final SocketChannel ch) throws Exception {
                socketChannel = ch;
                ch.config().setAllocator(PooledByteBufAllocator.DEFAULT);
                if (sslSettings.isEnabled()) {
                    SSLEngine engine = SSLContext.getDefault().createSSLEngine();
                    engine.setUseClientMode(true);

                    ch.pipeline().addFirst("ssl", new SslHandler(engine, false));
                }
                ch.pipeline().addLast(new InboundBufferHandler());
            }
        });

        // TODO: probably not a good idea.  And what about connect timeout?
        b.connect(address.getHost(), address.getPort()).syncUninterruptibly();
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        FutureAsyncCompletionHandler future = new FutureAsyncCompletionHandler();
        writeAsync(buffers, future);
        future.get();
    }

    @Override
    public void read(final ByteBuf buffer) throws IOException {
        FutureAsyncCompletionHandler future = new FutureAsyncCompletionHandler();
        readAsync(buffer, future);
        future.get();
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler handler) {
        io.netty.buffer.ByteBuf nettyBuffer = PooledByteBufAllocator.DEFAULT.directBuffer();
        for (ByteBuf cur : buffers) {
            nettyBuffer.writeBytes(cur.asNIO());
        }
        socketChannel.writeAndFlush(nettyBuffer).addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    handler.completed();
                } else {
                    handler.failed(future.cause());
                }
            }
        });
    }

    @Override
    public synchronized void readAsync(final ByteBuf buffer, final AsyncCompletionHandler handler) {
        while (buffer.hasRemaining()) {
            if (pendingException != null) {
                handler.failed(pendingException);
                return;
            }

            if (pendingInboundBuffers.isEmpty()) {
                pendingReader = new PendingReader(buffer, handler);
                return;
            }
            io.netty.buffer.ByteBuf next = pendingInboundBuffers.removeFirst();

            int bytesToRead = Math.min(next.readableBytes(), buffer.remaining());
            for (int i = 0; i < bytesToRead; i++) {
                buffer.put(next.readByte());
            }
            if (next.isReadable()) {
                pendingInboundBuffers.addFirst(next);
            } else {
                next.release();
            }
        }
        buffer.flip();
        handler.completed();
    }

    private synchronized void handledInboundBuffer(final io.netty.buffer.ByteBuf buffer) {
        pendingInboundBuffers.add(buffer.retain());
        handlePendingReader();
    }

    private synchronized void handleException(final Throwable t) {
        pendingException = t;
        handlePendingReader();
    }

    private void handlePendingReader() {
        if (pendingReader != null) {
            PendingReader localPendingReader = pendingReader;
            pendingReader = null;
            readAsync(localPendingReader.buffer, localPendingReader.handler);
        }
    }

    @Override
    public ServerAddress getAddress() {
        return address;
    }

    @Override
    public void close() {
        try {
            if (socketChannel != null) {
                socketChannel.close().syncUninterruptibly();   // TODO: not clear whether this should be a synchronous call
            }
        } finally {
            socketChannel = null;
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private class InboundBufferHandler extends SimpleChannelInboundHandler<io.netty.buffer.ByteBuf> {
        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final io.netty.buffer.ByteBuf buffer) throws Exception {
            handledInboundBuffer(buffer);
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable t) {
            handleException(t);
        }
    }

    private static final class PendingReader {
        private final ByteBuf buffer;
        private final AsyncCompletionHandler handler;

        private PendingReader(final ByteBuf buffer, final AsyncCompletionHandler handler) {
            this.buffer = buffer;
            this.handler = handler;
        }
    }

    private static final class FutureAsyncCompletionHandler implements AsyncCompletionHandler {
        private final CountDownLatch latch = new CountDownLatch(1);
        private Throwable t;

        public FutureAsyncCompletionHandler() {
        }

        @Override
        public void completed() {
            latch.countDown();
        }

        @Override
        public void failed(final Throwable t) {
            this.t = t;
            latch.countDown();
        }

        public void get() throws IOException {
            try {
                latch.await();
                if (t != null) {
                    if (t instanceof IOException) {
                        throw (IOException) t;
                    } else {
                        throw new MongoInternalException("Exception thrown from Netty Stream", t);
                    }
                }
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted", e);
            }
        }
    }
}
