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

package com.mongodb.connection.netty;

import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SSLSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.Stream;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.bson.ByteBuf;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A Stream implementation based on Netty 4.0.
 */
final class NettyStream implements Stream {
    private final ServerAddress address;
    private final ByteBufAllocator allocator;
    private final ChannelFuture channelFuture;
    private volatile boolean isClosed;

    private final LinkedList<io.netty.buffer.ByteBuf> pendingInboundBuffers = new LinkedList<io.netty.buffer.ByteBuf>();
    private PendingReader pendingReader;
    private Throwable pendingException;

    public NettyStream(final ServerAddress address, final SocketSettings settings, final SSLSettings sslSettings,
                       final EventLoopGroup workerGroup, final ByteBufAllocator allocator) {
        this.address = address;
        this.allocator = allocator;
        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);

        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, settings.getConnectTimeout(MILLISECONDS));
        b.option(ChannelOption.TCP_NODELAY, true);
        b.option(ChannelOption.SO_KEEPALIVE, settings.isKeepAlive());

        if (settings.getReceiveBufferSize() > 0) {
            b.option(ChannelOption.SO_RCVBUF, settings.getReceiveBufferSize());
        }
        if (settings.getSendBufferSize() > 0) {
            b.option(ChannelOption.SO_SNDBUF, settings.getSendBufferSize());
        }
        b.option(ChannelOption.ALLOCATOR, allocator);

        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(final SocketChannel ch) throws Exception {
                if (sslSettings.isEnabled()) {
                    SSLEngine engine = SSLContext.getDefault().createSSLEngine();
                    engine.setUseClientMode(true);

                    ch.pipeline().addFirst("ssl", new SslHandler(engine, false));
                }
                ch.pipeline().addLast("readTimeoutHandler", new ReadTimeoutHandler(settings.getReadTimeout(MILLISECONDS), MILLISECONDS));
                ch.pipeline().addLast(new InboundBufferHandler());
            }
        });

        this.channelFuture = b.connect(address.getHost(), address.getPort());
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return new NettyByteBuf(allocator.buffer(size, size));
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        FutureAsyncCompletionHandler<Void> future = new FutureAsyncCompletionHandler<Void>();
        writeAsync(buffers, future);
        future.get();
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        FutureAsyncCompletionHandler<ByteBuf> future = new FutureAsyncCompletionHandler<ByteBuf>();
        readAsync(numBytes, future);
        return future.get();
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        if (checkPendingException(handler)) {
            final CompositeByteBuf composite = PooledByteBufAllocator.DEFAULT.compositeBuffer();
            for (ByteBuf cur : buffers) {
                io.netty.buffer.ByteBuf byteBuf = ((NettyByteBuf) cur).asByteBuf();
                composite.addComponent(byteBuf.retain());
                composite.writerIndex(composite.writerIndex() + byteBuf.writerIndex());
            }

            final ChannelFutureListener futureListener = new ChannelFutureListener() {
                @Override
                public void operationComplete(final ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        handler.failed(future.cause());
                    } else {
                        handler.completed(null);
                    }
                }
            };

            if (!channelFuture.isDone()) {
                channelFuture.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(final ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            future.channel().writeAndFlush(composite).addListener(futureListener);
                        } else {
                            handler.failed(future.cause());
                        }
                    }
                });
            } else {
                channelFuture.channel().writeAndFlush(composite).addListener(futureListener);
            }
        }
    }

    @Override
    public synchronized void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        if (checkPendingException(handler)) {
            if (!hasBytesAvailable(numBytes)) {
                pendingReader = new PendingReader(numBytes, handler);
            } else {
                CompositeByteBuf composite = allocator.compositeBuffer(pendingInboundBuffers.size());
                int bytesNeeded = numBytes;
                for (Iterator<io.netty.buffer.ByteBuf> iter = pendingInboundBuffers.iterator(); iter.hasNext();) {
                    io.netty.buffer.ByteBuf next = iter.next();
                    int bytesNeededFromCurrentBuffer = Math.min(next.readableBytes(), bytesNeeded);
                    if (bytesNeededFromCurrentBuffer == next.readableBytes()) {
                        composite.addComponent(next);
                        iter.remove();
                    } else {
                        next.retain();
                        composite.addComponent(next.readSlice(bytesNeededFromCurrentBuffer));
                    }
                    composite.writerIndex(composite.writerIndex() + bytesNeededFromCurrentBuffer);
                    bytesNeeded -= bytesNeededFromCurrentBuffer;
                    if (bytesNeeded == 0) {
                        break;
                    }
                }
                NettyByteBuf buffer = new NettyByteBuf(composite);

                handler.completed(buffer.flip());
            }
        }
    }

    private boolean hasBytesAvailable(final int numBytes) {
        int bytesAvailable = 0;
        for (io.netty.buffer.ByteBuf cur : pendingInboundBuffers) {
            bytesAvailable += cur.readableBytes();
            if (bytesAvailable >= numBytes) {
                return true;
            }
        }
        return false;
    }

    private synchronized void handledInboundBuffer(final io.netty.buffer.ByteBuf buffer) {
        pendingInboundBuffers.add(buffer.retain());
        handlePendingReader();
    }

    private synchronized void handleException(final Throwable t) {
        pendingException = t;
        handlePendingReader();
    }

    private boolean checkPendingException(final AsyncCompletionHandler<?> handler) {
        if (pendingException != null) {
            handler.failed(pendingException);
            return false;
        } else {
            return true;
        }
    }

    private void handlePendingReader() {
        if (pendingReader != null) {
            PendingReader localPendingReader = pendingReader;
            pendingReader = null;
            readAsync(localPendingReader.numBytes, localPendingReader.handler);
        }
    }

    @Override
    public ServerAddress getAddress() {
        return address;
    }

    @Override
    public void close() {
        isClosed = true;
        if (channelFuture.isSuccess()) {
            channelFuture.channel().close();
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
            if (t instanceof ReadTimeoutException) {
                handleException(new MongoSocketReadTimeoutException("Timeout while receiving message", address, t));
            } else {
                handleException(t);
            }
            ctx.close();
        }
    }

    private static final class PendingReader {
        private final int numBytes;
        private final AsyncCompletionHandler<ByteBuf> handler;

        private PendingReader(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
            this.numBytes = numBytes;
            this.handler = handler;
        }
    }

    private static final class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile T t;
        private volatile Throwable throwable;

        public FutureAsyncCompletionHandler() {
        }

        @Override
        public void completed(final T t) {
            this.t = t;
            latch.countDown();
        }

        @Override
        public void failed(final Throwable t) {
            this.throwable = t;
            latch.countDown();
        }

        public T get() throws IOException {
            try {
                latch.await();
                if (throwable != null) {
                    if (throwable instanceof IOException) {
                        throw (IOException) throwable;
                    } else if (throwable instanceof MongoSocketReadTimeoutException) {
                        throw (MongoSocketReadTimeoutException) throwable;
                    } else if (throwable instanceof MongoSocketOpenException) {
                        throw (MongoSocketOpenException) throwable;
                    } else {
                        throw new MongoInternalException("Exception thrown from Netty Stream", throwable);
                    }
                }
                return t;
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted", e);
            }
        }
    }
}
