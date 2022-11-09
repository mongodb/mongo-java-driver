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

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.Stream;
import com.mongodb.lang.Nullable;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutException;
import org.bson.ByteBuf;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.connection.SslHelper.enableHostNameVerification;
import static com.mongodb.internal.connection.SslHelper.enableSni;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A Stream implementation based on Netty 4.0.
 * Just like it is for the {@link java.nio.channels.AsynchronousSocketChannel},
 * concurrent pending<sup>1</sup> readers
 * (whether {@linkplain #read(int, int) synchronous} or {@linkplain #readAsync(int, AsyncCompletionHandler) asynchronous})
 * are not supported by {@link NettyStream}.
 * However, this class does not have a fail-fast mechanism checking for such situations.
 * <hr>
 * <sup>1</sup>We cannot simply say that read methods are not allowed be run concurrently because strictly speaking they are allowed,
 * as explained below.
 * <pre>{@code
 * NettyStream stream = ...;
 * stream.readAsync(1, new AsyncCompletionHandler<ByteBuf>() {//inv1
 *  @Override
 *  public void completed(ByteBuf o) {
 *      stream.readAsync(//inv2
 *              1, ...);//ret2
 *  }
 *
 *  @Override
 *  public void failed(Throwable t) {
 *  }
 * });//ret1
 * }</pre>
 * Arrows on the diagram below represent happens-before relations.
 * <pre>{@code
 * int1 -> inv2 -> ret2
 *      \--------> ret1
 * }</pre>
 * As shown on the diagram, the method {@link #readAsync(int, AsyncCompletionHandler)} runs concurrently with
 * itself in the example above. However, there are no concurrent pending readers because the second operation
 * is invoked after the first operation has completed reading despite the method has not returned yet.
 */
final class NettyStream implements Stream {
    private static final byte NO_SCHEDULE_TIME = 0;
    private final ServerAddress address;
    private final SocketSettings settings;
    private final SslSettings sslSettings;
    private final EventLoopGroup workerGroup;
    private final Class<? extends SocketChannel> socketChannelClass;
    private final ByteBufAllocator allocator;
    @Nullable
    private final SslContext sslContext;

    private boolean isClosed;
    private volatile Channel channel;

    private final LinkedList<io.netty.buffer.ByteBuf> pendingInboundBuffers = new LinkedList<>();
    /* The fields pendingReader, pendingException are always written/read inside synchronized blocks
     * that use the same NettyStream object, so they can be plain.*/
    private PendingReader pendingReader;
    private Throwable pendingException;
    /* The fields readTimeoutTask, readTimeoutMillis are each written only in the ChannelInitializer.initChannel method
     * (in addition to the write of the default value and the write by variable initializers),
     * and read only when NettyStream users read data, or Netty event loop handles incoming data.
     * Since actions done by the ChannelInitializer.initChannel method
     * are ordered (in the happens-before order) before user read actions and before event loop actions that handle incoming data,
     * these fields can be plain.*/
    @Nullable
    private ReadTimeoutTask readTimeoutTask;
    private long readTimeoutMillis = NO_SCHEDULE_TIME;

    NettyStream(final ServerAddress address, final SocketSettings settings, final SslSettings sslSettings, final EventLoopGroup workerGroup,
                final Class<? extends SocketChannel> socketChannelClass, final ByteBufAllocator allocator,
                @Nullable final SslContext sslContext) {
        this.address = address;
        this.settings = settings;
        this.sslSettings = sslSettings;
        this.workerGroup = workerGroup;
        this.socketChannelClass = socketChannelClass;
        this.allocator = allocator;
        this.sslContext = sslContext;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return new NettyByteBuf(allocator.buffer(size, size));
    }

    @Override
    public void open() throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<>();
        openAsync(handler);
        handler.get();
    }

    @Override
    public void openAsync(final AsyncCompletionHandler<Void> handler) {
        Queue<SocketAddress> socketAddressQueue;

        try {
            socketAddressQueue = new LinkedList<>(address.getSocketAddresses());
        } catch (Throwable t) {
            handler.failed(t);
            return;
        }

        initializeChannel(handler, socketAddressQueue);
    }

    private void initializeChannel(final AsyncCompletionHandler<Void> handler, final Queue<SocketAddress> socketAddressQueue) {
        if (socketAddressQueue.isEmpty()) {
            handler.failed(new MongoSocketException("Exception opening socket", getAddress()));
        } else {
            SocketAddress nextAddress = socketAddressQueue.poll();

            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(socketChannelClass);

            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, settings.getConnectTimeout(MILLISECONDS));
            bootstrap.option(ChannelOption.TCP_NODELAY, true);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

            if (settings.getReceiveBufferSize() > 0) {
                bootstrap.option(ChannelOption.SO_RCVBUF, settings.getReceiveBufferSize());
            }
            if (settings.getSendBufferSize() > 0) {
                bootstrap.option(ChannelOption.SO_SNDBUF, settings.getSendBufferSize());
            }
            bootstrap.option(ChannelOption.ALLOCATOR, allocator);

            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(final SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (sslSettings.isEnabled()) {
                        addSslHandler(ch);
                    }

                    int readTimeout = settings.getReadTimeout(MILLISECONDS);
                    if (readTimeout > NO_SCHEDULE_TIME) {
                        readTimeoutMillis = readTimeout;
                        /* We need at least one handler before (in the inbound evaluation order) the InboundBufferHandler,
                         * so that we can fire exception events (they are inbound events) using its context and the InboundBufferHandler
                         * receives them. SslHandler is not always present, so adding a NOOP handler.*/
                        pipeline.addLast(new ChannelInboundHandlerAdapter());
                        readTimeoutTask = new ReadTimeoutTask(pipeline.lastContext());
                    }

                    pipeline.addLast(new InboundBufferHandler());
                }
            });
            ChannelFuture channelFuture = bootstrap.connect(nextAddress);
            channelFuture.addListener(new OpenChannelFutureListener(socketAddressQueue, channelFuture, handler));
        }
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        FutureAsyncCompletionHandler<Void> future = new FutureAsyncCompletionHandler<>();
        writeAsync(buffers, future);
        future.get();
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        return read(numBytes, 0);
    }

    @Override
    public boolean supportsAdditionalTimeout() {
        return true;
    }

    @Override
    public ByteBuf read(final int numBytes, final int additionalTimeoutMillis) throws IOException {
        isTrueArgument("additionalTimeoutMillis must not be negative", additionalTimeoutMillis >= 0);
        FutureAsyncCompletionHandler<ByteBuf> future = new FutureAsyncCompletionHandler<>();
        readAsync(numBytes, future, combinedTimeout(readTimeoutMillis, additionalTimeoutMillis));
        return future.get();
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        CompositeByteBuf composite = PooledByteBufAllocator.DEFAULT.compositeBuffer();
        for (ByteBuf cur : buffers) {
            composite.addComponent(true, ((NettyByteBuf) cur).asByteBuf());
        }

        channel.writeAndFlush(composite).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    handler.failed(future.cause());
                } else {
                    handler.completed(null);
                }
            }
        });
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        readAsync(numBytes, handler, readTimeoutMillis);
    }

    /**
     * @param numBytes Must be equal to {@link #pendingReader}{@code .numBytes} when called by a Netty channel handler.
     * @param handler Must be equal to {@link #pendingReader}{@code .handler} when called by a Netty channel handler.
     * @param readTimeoutMillis Must be equal to {@link #NO_SCHEDULE_TIME} when called by a Netty channel handler.
     *                          Timeouts may be scheduled only by the public read methods. Taking into account that concurrent pending
     *                          readers are not allowed, there must not be a situation when threads attempt to schedule a timeout
     *                          before the previous one is either cancelled or completed.
     */
    private void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler, final long readTimeoutMillis) {
        ByteBuf buffer = null;
        Throwable exceptionResult = null;
        synchronized (this) {
            exceptionResult = pendingException;
            if (exceptionResult == null) {
                if (!hasBytesAvailable(numBytes)) {
                    if (pendingReader == null) {//called by a public read method
                        pendingReader = new PendingReader(numBytes, handler, scheduleReadTimeout(readTimeoutTask, readTimeoutMillis));
                    }
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
                    buffer = new NettyByteBuf(composite).flip();
                }
            }
            if (!(exceptionResult == null && buffer == null)//the read operation has completed
                    && pendingReader != null) {//we need to clear the pending reader
                cancel(pendingReader.timeout);
                this.pendingReader = null;
            }
        }
        if (exceptionResult != null) {
            handler.failed(exceptionResult);
        }
        if (buffer != null) {
            handler.completed(buffer);
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

    private void handleReadResponse(final io.netty.buffer.ByteBuf buffer, final Throwable t) {
        PendingReader localPendingReader = null;
        synchronized (this) {
            if (buffer != null) {
                pendingInboundBuffers.add(buffer.retain());
            } else {
                pendingException = t;
            }
            localPendingReader = pendingReader;
        }

        if (localPendingReader != null) {
            //timeouts may be scheduled only by the public read methods
            readAsync(localPendingReader.numBytes, localPendingReader.handler, NO_SCHEDULE_TIME);
        }
    }

    @Override
    public ServerAddress getAddress() {
        return address;
    }

    @Override
    public synchronized void close() {
        isClosed = true;
        if (channel != null) {
            channel.close();
            channel = null;
        }
        for (Iterator<io.netty.buffer.ByteBuf> iterator = pendingInboundBuffers.iterator(); iterator.hasNext();) {
            io.netty.buffer.ByteBuf nextByteBuf = iterator.next();
            iterator.remove();
            nextByteBuf.release();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    public SocketSettings getSettings() {
        return settings;
    }

    public SslSettings getSslSettings() {
        return sslSettings;
    }

    public EventLoopGroup getWorkerGroup() {
        return workerGroup;
    }

    public Class<? extends SocketChannel> getSocketChannelClass() {
        return socketChannelClass;
    }

    public ByteBufAllocator getAllocator() {
        return allocator;
    }

    private void addSslHandler(final SocketChannel channel) {
        SSLEngine engine;
        if (sslContext == null) {
            SSLContext sslContext;
            try {
                sslContext = (sslSettings.getContext() == null) ? SSLContext.getDefault() : sslSettings.getContext();
            } catch (NoSuchAlgorithmException e) {
                throw new MongoClientException("Unable to create standard SSLContext", e);
            }
            engine = sslContext.createSSLEngine(address.getHost(), address.getPort());
        } else {
            engine = sslContext.newEngine(channel.alloc(), address.getHost(), address.getPort());
        }
        engine.setUseClientMode(true);
        SSLParameters sslParameters = engine.getSSLParameters();
        enableSni(address.getHost(), sslParameters);
        if (!sslSettings.isInvalidHostNameAllowed()) {
            enableHostNameVerification(sslParameters);
        }
        engine.setSSLParameters(sslParameters);
        channel.pipeline().addFirst("ssl", new SslHandler(engine, false));
    }

    private class InboundBufferHandler extends SimpleChannelInboundHandler<io.netty.buffer.ByteBuf> {
        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final io.netty.buffer.ByteBuf buffer) throws Exception {
            handleReadResponse(buffer, null);
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable t) {
            if (t instanceof ReadTimeoutException) {
                handleReadResponse(null, new MongoSocketReadTimeoutException("Timeout while receiving message", address, t));
            } else {
                handleReadResponse(null, t);
            }
            ctx.close();
        }
    }

    private static final class PendingReader {
        private final int numBytes;
        private final AsyncCompletionHandler<ByteBuf> handler;
        @Nullable
        private final ScheduledFuture<?> timeout;

        private PendingReader(
                final int numBytes, final AsyncCompletionHandler<ByteBuf> handler, @Nullable final ScheduledFuture<?> timeout) {
            this.numBytes = numBytes;
            this.handler = handler;
            this.timeout = timeout;
        }
    }

    private static final class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile T t;
        private volatile Throwable throwable;

        FutureAsyncCompletionHandler() {
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
                    } else if (throwable instanceof MongoException) {
                        throw (MongoException) throwable;
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

    private class OpenChannelFutureListener implements ChannelFutureListener {
        private final Queue<SocketAddress> socketAddressQueue;
        private final ChannelFuture channelFuture;
        private final AsyncCompletionHandler<Void> handler;

        OpenChannelFutureListener(final Queue<SocketAddress> socketAddressQueue, final ChannelFuture channelFuture,
                                  final AsyncCompletionHandler<Void> handler) {
            this.socketAddressQueue = socketAddressQueue;
            this.channelFuture = channelFuture;
            this.handler = handler;
        }

        @Override
        public void operationComplete(final ChannelFuture future) {
            synchronized (NettyStream.this) {
                if (future.isSuccess()) {
                    if (isClosed) {
                        channelFuture.channel().close();
                    } else {
                        channel = channelFuture.channel();
                        channel.closeFuture().addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(final ChannelFuture future) {
                                handleReadResponse(null, new IOException("The connection to the server was closed"));
                            }
                        });
                    }
                    handler.completed(null);
                } else {
                    if (isClosed) {
                        handler.completed(null);
                    } else if (socketAddressQueue.isEmpty()) {
                        handler.failed(new MongoSocketOpenException("Exception opening socket", getAddress(), future.cause()));
                    } else {
                        initializeChannel(handler, socketAddressQueue);
                    }
                }
            }
        }
    }

    private static void cancel(@Nullable final Future<?> f) {
        if (f != null) {
            f.cancel(false);
        }
    }

    private static long combinedTimeout(final long timeout, final int additionalTimeout) {
        if (timeout == NO_SCHEDULE_TIME) {
            return NO_SCHEDULE_TIME;
        } else {
            return Math.addExact(timeout, additionalTimeout);
        }
    }

    private static ScheduledFuture<?> scheduleReadTimeout(@Nullable final ReadTimeoutTask readTimeoutTask, final long timeoutMillis) {
        if (timeoutMillis == NO_SCHEDULE_TIME) {
            return null;
        } else {
            //assert readTimeoutTask != null : "readTimeoutTask must be initialized if read timeouts are enabled";
            return readTimeoutTask.schedule(timeoutMillis);
        }
    }

    @ThreadSafe
    private static final class ReadTimeoutTask implements Runnable {
        private final ChannelHandlerContext ctx;

        private ReadTimeoutTask(final ChannelHandlerContext timeoutChannelHandlerContext) {
            ctx = timeoutChannelHandlerContext;
        }

        @Override
        public void run() {
            try {
                if (ctx.channel().isOpen()) {
                    ctx.fireExceptionCaught(ReadTimeoutException.INSTANCE);
                    ctx.close();
                }
            } catch (final Throwable t) {
                ctx.fireExceptionCaught(t);
            }
        }

        private ScheduledFuture<?> schedule(final long timeoutMillis) {
            //assert timeoutMillis > 0 : timeoutMillis;
            return ctx.executor().schedule(this, timeoutMillis, MILLISECONDS);
        }
    }
}
