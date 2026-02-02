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

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.Stream;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.InetAddressResolver;
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
import io.netty.handler.timeout.WriteTimeoutHandler;
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
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.Locks.withLock;
import static com.mongodb.internal.connection.ServerAddressHelper.getSocketAddresses;
import static com.mongodb.internal.connection.SslHelper.enableHostNameVerification;
import static com.mongodb.internal.connection.SslHelper.enableSni;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A Stream implementation based on Netty 4.0.
 * Just like it is for the {@link java.nio.channels.AsynchronousSocketChannel},
 * concurrent pending<sup>1</sup> readers
 * (whether {@linkplain #read(int, OperationContext) synchronous} or
 * {@linkplain #readAsync(int, OperationContext, AsyncCompletionHandler) asynchronous})
 * are not supported by {@link NettyStream}.
 * However, this class does not have a fail-fast mechanism checking for such situations.
 *
 * <h2>ByteBuf Ownership and Reference Counting</h2>
 * <p>This class manages Netty {@link io.netty.buffer.ByteBuf} instances which use reference counting for memory management.
 * The following ownership rules apply:</p>
 * <ul>
 *   <li><b>Inbound buffers from Netty:</b> When Netty delivers a buffer via {@link InboundBufferHandler#channelRead0},
 *       the buffer is initially owned by Netty. To keep the buffer, we actively call {@link io.netty.buffer.ByteBuf#retain()}
 *       to increment its reference count before adding it to {@link #pendingInboundBuffers}.</li>
 *   <li><b>{@link #pendingInboundBuffers}:</b> All buffers in this queue have been retained and are owned by this class.
 *       They must be released when consumed or when the stream is closed.</li>
 *   <li><b>Read completion:</b> When a read operation completes successfully, ownership of the returned {@link ByteBuf}
 *       transfers to the caller (via {@link AsyncCompletionHandler#completed}). The caller is responsible for releasing it.</li>
 *   <li><b>Handler exceptions:</b> If {@link AsyncCompletionHandler#completed} throws an exception, ownership was not
 *       transferred, and this class releases the buffer.</li>
 *   <li><b>Stream closure:</b> When {@link #close()} is called, all buffers in {@link #pendingInboundBuffers} are released.
 *       Callers must ensure any previously returned buffers are released before calling close.</li>
 * </ul>
 *
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
 * As shown on the diagram, the method {@link #readAsync(int, OperationContext, AsyncCompletionHandler)} runs concurrently with
 * itself in the example above. However, there are no concurrent pending readers because the second operation
 * is invoked after the first operation has completed reading despite the method has not returned yet.
 */
final class NettyStream implements Stream {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private static final byte NO_SCHEDULE_TIME = 0;
    private final ServerAddress address;
    private final InetAddressResolver inetAddressResolver;
    private final SocketSettings settings;
    private final SslSettings sslSettings;
    private final EventLoopGroup workerGroup;
    private final Class<? extends SocketChannel> socketChannelClass;
    private final ByteBufAllocator allocator;
    @Nullable
    private final SslContext sslContext;

    private boolean isClosed;
    private volatile Channel channel;

    /**
     * Queue of inbound buffers received from Netty that have not yet been consumed by read operations.
     *
     * <p><b>Ownership:</b> All buffers in this queue have been {@link io.netty.buffer.ByteBuf#retain() retained}
     * and are owned by this class. When a buffer is removed from this queue, the remover takes ownership and
     * is responsible for either:</p>
     * <ul>
     *   <li>Transferring ownership to a caller (e.g., via {@link AsyncCompletionHandler#completed}), or</li>
     *   <li>Releasing the buffer via {@link io.netty.buffer.ByteBuf#release()}</li>
     * </ul>
     *
     * <p>When the stream is {@link #close() closed}, all remaining buffers are released.</p>
     */
    private final LinkedList<io.netty.buffer.ByteBuf> pendingInboundBuffers = new LinkedList<>();
    private final Lock lock = new ReentrantLock();
    // access to the fields `pendingReader`, `pendingException` is guarded by `lock`
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

    NettyStream(final ServerAddress address, final InetAddressResolver inetAddressResolver, final SocketSettings settings,
            final SslSettings sslSettings, final EventLoopGroup workerGroup,
            final Class<? extends SocketChannel> socketChannelClass, final ByteBufAllocator allocator,
            @Nullable final SslContext sslContext) {
        this.address = address;
        this.inetAddressResolver = inetAddressResolver;
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
    public void open(final OperationContext operationContext) throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<>();
        openAsync(operationContext, handler);
        handler.get();
    }

    @Override
    public void openAsync(final OperationContext operationContext, final AsyncCompletionHandler<Void> handler) {
        Queue<SocketAddress> socketAddressQueue;

        try {
            socketAddressQueue = new LinkedList<>(getSocketAddresses(address, inetAddressResolver));
        } catch (Throwable t) {
            handler.failed(t);
            return;
        }

        initializeChannel(operationContext, handler, socketAddressQueue);
    }

    private void initializeChannel(final OperationContext operationContext, final AsyncCompletionHandler<Void> handler,
            final Queue<SocketAddress> socketAddressQueue) {
        if (socketAddressQueue.isEmpty()) {
            handler.failed(new MongoSocketException("Exception opening socket", getAddress()));
        } else {
            SocketAddress nextAddress = socketAddressQueue.poll();

            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(socketChannelClass);
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                    operationContext.getTimeoutContext().getConnectTimeoutMs());
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

                    /* We need at least one handler before (in the inbound evaluation order) the InboundBufferHandler,
                     * so that we can fire exception events (they are inbound events) using its context and the InboundBufferHandler
                     * receives them. SslHandler is not always present, so adding a NOOP handler.*/
                    pipeline.addLast("ChannelInboundHandlerAdapter",  new ChannelInboundHandlerAdapter());
                    readTimeoutTask = new ReadTimeoutTask(pipeline.lastContext());
                    pipeline.addLast("InboundBufferHandler", new InboundBufferHandler());
                }
            });
            ChannelFuture channelFuture = bootstrap.connect(nextAddress);
            channelFuture.addListener(new OpenChannelFutureListener(operationContext, socketAddressQueue, channelFuture, handler));
        }
    }

    @Override
    public void write(final List<ByteBuf> buffers, final OperationContext operationContext) throws IOException {
        validateConnectionState();
        FutureAsyncCompletionHandler<Void> future = new FutureAsyncCompletionHandler<>();
        writeAsync(buffers, operationContext, future);
        future.get();
    }

    @Override
    public ByteBuf read(final int numBytes, final OperationContext operationContext) throws IOException {
        FutureAsyncCompletionHandler<ByteBuf> future = new FutureAsyncCompletionHandler<>();
        readAsync(numBytes, future, operationContext.getTimeoutContext().getReadTimeoutMS());
        return future.get();
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final OperationContext operationContext,
            final AsyncCompletionHandler<Void> handler) {
        // Early validation before allocating resources
        if (isClosed) {
            handler.failed(new MongoSocketException("Stream is closed", address));
            return;
        }
        Channel localChannel = channel;
        if (localChannel == null || !localChannel.isActive()) {
            handler.failed(new MongoSocketException("Channel is not active", address));
            return;
        }

        CompositeByteBuf composite = PooledByteBufAllocator.DEFAULT.compositeBuffer();

        try {
            for (ByteBuf cur : buffers) {
                // The Netty framework releases `CompositeByteBuf` after writing
                // (see https://netty.io/wiki/reference-counted-objects.html#outbound-messages),
                // which results in the buffer we pass to `CompositeByteBuf.addComponent` being released.
                // However, `CompositeByteBuf.addComponent` does not retain this buffer,
                // which means we must retain it to conform to the `Stream.writeAsync` contract.
                composite.addComponent(true, ((NettyByteBuf) cur).asByteBuf().retain());
            }

            long writeTimeoutMS = operationContext.getTimeoutContext().getWriteTimeoutMS();
            final Optional<WriteTimeoutHandler> writeTimeoutHandler = addWriteTimeoutHandler(localChannel, writeTimeoutMS);

            localChannel.writeAndFlush(composite).addListener((ChannelFutureListener) future -> {
                writeTimeoutHandler.map(w -> localChannel.pipeline().remove(w));

                if (!future.isSuccess()) {
                    handler.failed(future.cause());
                } else {
                    handler.completed(null);
                }
            });
        } catch (Throwable t) {
            // If we fail before submitting the write, release the composite
            composite.release();
            handler.failed(t);
        }
    }

    private Optional<WriteTimeoutHandler> addWriteTimeoutHandler(final Channel channel, final long writeTimeoutMS) {
        if (writeTimeoutMS != NO_SCHEDULE_TIME) {
            WriteTimeoutHandler writeTimeoutHandler = new WriteTimeoutHandler(writeTimeoutMS, MILLISECONDS);
            channel.pipeline().addBefore("ChannelInboundHandlerAdapter", "WriteTimeoutHandler", writeTimeoutHandler);
            return Optional.of(writeTimeoutHandler);
        }
        return Optional.empty();
    }

    @Override
    public void readAsync(final int numBytes, final OperationContext operationContext, final AsyncCompletionHandler<ByteBuf> handler) {
        readAsync(numBytes, handler, operationContext.getTimeoutContext().getReadTimeoutMS());
    }

    /**
     * @param numBytes Must be equal to {@link #pendingReader}{@code .numBytes} when called by a Netty channel handler.
     * @param handler Must be equal to {@link #pendingReader}{@code .handler} when called by a Netty channel handler.
     * @param readTimeoutMillis Must be equal to {@link #NO_SCHEDULE_TIME} when called by a Netty channel handler.
     *                          Timeouts may be scheduled only by the public read methods. Taking into account that concurrent pending
     *                          readers are not allowed, there must not be a situation when threads attempt to schedule a timeout
     *                          before the previous one is either cancelled or completed.
     *
     * <h3>Buffer Ownership</h3>
     * <p>When this method completes a read operation successfully:</p>
     * <ul>
     *   <li>Buffers are removed from {@link #pendingInboundBuffers} and assembled into a composite buffer</li>
     *   <li>Ownership of the composite buffer is transferred to the handler via {@link AsyncCompletionHandler#completed}</li>
     *   <li>If the handler throws an exception, the buffer is released by {@link #invokeHandlerWithBuffer}</li>
     * </ul>
     * <p>If an exception occurs during buffer assembly, the partially-built composite is released before propagating the exception.</p>
     */
    private void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler, final long readTimeoutMillis) {
        ByteBuf buffer = null;
        Throwable exceptionResult;
        lock.lock();
        try {

            if (pendingException == null) {
                if (isClosed) {
                    pendingException = new MongoSocketException("Stream was closed", address);
                    // Release any pending buffers that were retained before stream was closed
                    releaseAllPendingInboundBuffers();
                } else if (channel == null || !channel.isActive()) {
                    pendingException = new MongoSocketException("Channel is not active", address);
                    // Release any pending buffers that were retained before channel became inactive
                    releaseAllPendingInboundBuffers();
                } else if (!hasBytesAvailable(numBytes)) {
                    if (pendingReader == null) {//called by a public read method
                        pendingReader = new PendingReader(numBytes, handler, scheduleReadTimeout(readTimeoutTask, readTimeoutMillis));
                    }
                } else {
                    CompositeByteBuf composite = allocator.compositeBuffer(pendingInboundBuffers.size());
                    try {
                        int bytesNeeded = numBytes;
                        for (Iterator<io.netty.buffer.ByteBuf> iter = pendingInboundBuffers.iterator(); iter.hasNext();) {
                            io.netty.buffer.ByteBuf next = iter.next();
                            int bytesNeededFromCurrentBuffer = Math.min(next.readableBytes(), bytesNeeded);
                            if (bytesNeededFromCurrentBuffer == next.readableBytes()) {
                                iter.remove(); // Remove BEFORE adding to composite
                                composite.addComponent(true, next);
                            } else {
                                composite.addComponent(true, next.readRetainedSlice(bytesNeededFromCurrentBuffer));
                            }

                            bytesNeeded -= bytesNeededFromCurrentBuffer;
                            if (bytesNeeded == 0) {
                                break;
                            }
                        }
                        buffer = new NettyByteBuf(composite).flip();
                    } catch (Throwable t) {
                        composite.release();
                        pendingException = t;
                    }
                }
            }

            exceptionResult = pendingException;
            if (!(exceptionResult == null && buffer == null) //the read operation has completed
                    && pendingReader != null) { //we need to clear the pending reader
                cancel(pendingReader.timeout);
                this.pendingReader = null;
            }
        } finally {
            lock.unlock();
        }

        if (exceptionResult != null) {
            handler.failed(exceptionResult);
        } else if (buffer != null) {
            invokeHandlerWithBuffer(buffer, handler);
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

    private void handleReadResponse(@Nullable final io.netty.buffer.ByteBuf buffer, @Nullable final Throwable t) {
        PendingReader localPendingReader = withLock(lock, () -> {
            if (buffer != null) {
                if (isClosed) {
                    pendingException = new MongoSocketException("Received data after the stream was closed.", address);
                    // Do not retain the buffer since we're not storing it - let it be released by the caller
                } else if (channel == null || !channel.isActive()) {
                    pendingException = new MongoSocketException("Channel is not active during read", address);
                    // Do not retain the buffer - channel is unusable
                } else {
                    pendingInboundBuffers.add(buffer.retain());
                }
            } else {
                pendingException = t;
            }
            return pendingReader;
        });

        if (localPendingReader != null) {
            //timeouts may be scheduled only by the public read methods
            readAsync(localPendingReader.numBytes, localPendingReader.handler, NO_SCHEDULE_TIME);
        }
    }

    @Override
    public ServerAddress getAddress() {
        return address;
    }

    /**
     * Closes this stream and releases all resources.
     *
     * <h3>Buffer Cleanup</h3>
     * <p>All buffers remaining in {@link #pendingInboundBuffers} are released synchronously while holding
     * the lock to prevent race conditions. Buffers are forcefully released (all reference counts dropped)
     * to prevent silent leaks.</p>
     *
     * <h3>Write Buffer Handling</h3>
     * <p>Write buffers use retainedSlice() which creates independent reference counts for Netty and the caller.
     * This eliminates the need for explicit tracking - each party manages its own buffer lifecycle independently.</p>
     *
     * <h3>Channel Cleanup</h3>
     * <p>If a channel exists, it is closed asynchronously. The async listener performs defensive cleanup
     * to ensure any buffers added during the close process are also released. This provides defense-in-depth
     * against race conditions.</p>
     *
     * <p><b>Important:</b> Callers must ensure that any {@link ByteBuf} instances previously returned by
     * {@link #read} or {@link #readAsync} have been released before calling this method. This class does not
     * track buffers after ownership has been transferred to callers.</p>
     */
    @Override
    public void close() {
        Channel channelToClose = withLock(lock, () ->  {
            if (!isClosed) {
                isClosed = true;

                // Clean up all pending inbound buffers synchronously while holding the lock
                // This prevents race conditions where buffers might be added during close
                releaseAllPendingInboundBuffers();

                // Save channel reference for async close, then null it out
                Channel localChannel = channel;
                channel = null;
                return localChannel;
            }
            return null;
        });


        // Close the channel outside the lock to avoid potential deadlocks
        if (channelToClose != null) {
            channelToClose.close().addListener((ChannelFutureListener) future -> {
                // Defensive cleanup: release any buffers that might have been added during close
                // This is safe because isClosed=true prevents handleReadResponse from retaining new buffers
                withLock(lock, () -> {
                    try {
                        releaseAllPendingInboundBuffers();
                    } catch (Throwable t) {
                        // Log but don't propagate - we're in an async callback
                        LOGGER.warn("Exception while releasing buffers in channel close listener", t);
                    }
                });
            });
        }
    }

    /**
     * Releases all buffers in {@link #pendingInboundBuffers} and clears the list.
     * This method must be called while holding {@link #lock}.
     *
     * <p>Each buffer is forcefully released by dropping all its reference counts (not just one)
     * to prevent silent leaks in case of reference counting errors elsewhere.</p>
     *
     * <p>This method is idempotent - it can be safely called multiple times.</p>
     */
    private void releaseAllPendingInboundBuffers() {
        int releasedCount = 0;
        int errorCount = 0;

        for (io.netty.buffer.ByteBuf buffer : pendingInboundBuffers) {
            try {
                int refCnt = buffer.refCnt();
                if (refCnt > 0) {
                    buffer.release(refCnt);
                    releasedCount++;
                }
            } catch (Throwable t) {
                errorCount++;
                // Log but continue releasing other buffers - we want to release all buffers even if one fails
                LOGGER.warn("Exception while releasing buffer with refCount " + buffer.refCnt(), t);
            }
        }
        pendingInboundBuffers.clear();

        if (LOGGER.isDebugEnabled() && (releasedCount > 0 || errorCount > 0)) {
            LOGGER.debug(String.format("Released %d buffers for %s (%d errors)", releasedCount, address, errorCount));
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

    /**
     * Validates that the stream is open and the channel is active.
     *
     * @throws MongoSocketException if the stream is closed or the channel is not active
     */
    private void validateConnectionState() throws MongoSocketException {
        if (isClosed) {
            throw new MongoSocketException("Stream is closed", address);
        }
        Channel localChannel = channel;
        if (localChannel == null || !localChannel.isActive()) {
            throw new MongoSocketException("Channel is not active", address);
        }
    }

    /**
     * Invokes the handler with the buffer, ensuring the buffer is released if the handler throws an exception.
     *
     * <h3>Ownership Transfer Protocol</h3>
     * <p>This method implements a safe ownership transfer:</p>
     * <ol>
     *   <li>The buffer is passed to {@link AsyncCompletionHandler#completed}</li>
     *   <li>If the handler returns normally, ownership has been successfully transferred to the handler/caller</li>
     *   <li>If the handler throws an exception, ownership was NOT transferred, so this method releases the buffer
     *       before re-throwing the exception</li>
     * </ol>
     *
     * <p>This ensures that buffers are never leaked, regardless of whether the handler succeeds or fails.</p>
     *
     * @param buffer  The buffer to pass to the handler. Must not be null. Ownership is transferred to the handler
     *                on successful completion.
     * @param handler The handler to invoke with the buffer.
     * @throws RuntimeException if the handler throws an exception (after releasing the buffer)
     */
    private void invokeHandlerWithBuffer(final ByteBuf buffer, final AsyncCompletionHandler<ByteBuf> handler) {
        try {
            handler.completed(buffer);
        } catch (Throwable t) {
            // Handler threw an exception, so it didn't take ownership - release the buffer
            if (buffer.getReferenceCount() > 0) {
                buffer.release();
            }
            throw t;
        }
    }

    private void addSslHandler(final SocketChannel channel) {
        SSLEngine engine;
        if (sslContext == null) {
            SSLContext sslContext;
            try {
                sslContext = ofNullable(sslSettings.getContext()).orElse(SSLContext.getDefault());
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
        protected void channelRead0(final ChannelHandlerContext ctx, final io.netty.buffer.ByteBuf buffer) {
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
        public void completed(@Nullable final T t) {
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
                throw interruptAndCreateMongoInterruptedException("Interrupted", e);
            }
        }
    }

    private class OpenChannelFutureListener implements ChannelFutureListener {
        private final Queue<SocketAddress> socketAddressQueue;
        private final ChannelFuture channelFuture;
        private final AsyncCompletionHandler<Void> handler;
        private final OperationContext operationContext;

        OpenChannelFutureListener(final OperationContext operationContext,
                final Queue<SocketAddress> socketAddressQueue, final ChannelFuture channelFuture,
                final AsyncCompletionHandler<Void> handler) {
            this.operationContext = operationContext;
            this.socketAddressQueue = socketAddressQueue;
            this.channelFuture = channelFuture;
            this.handler = handler;
        }

        @Override
        public void operationComplete(final ChannelFuture future) {
            withLock(lock, () -> {
                if (future.isSuccess()) {
                    Channel newChannel = channelFuture.channel();
                    if (isClosed) {
                        // Monitor closed during connection - clean up immediately
                        if (newChannel != null) {
                            newChannel.close();
                        }
                        handler.completed(null);
                    } else if (newChannel == null || !newChannel.isActive()) {
                        // Channel invalid - treat as failure
                        handler.failed(new MongoSocketException("Channel is not active after connection", address));
                    } else {
                        channel = newChannel;
                        channel.closeFuture().addListener((ChannelFutureListener) future1 ->
                            handleReadResponse(null, new IOException("The connection to the server was closed")));
                        handler.completed(null);
                    }
                } else {
                    if (isClosed) {
                        handler.completed(null);
                    } else if (socketAddressQueue.isEmpty()) {
                        handler.failed(new MongoSocketOpenException("Exception opening socket", getAddress(), future.cause()));
                    } else {
                        initializeChannel(operationContext, handler, socketAddressQueue);
                    }
                }
            });
        }
    }

    private static void cancel(@Nullable final Future<?> f) {
        if (f != null) {
            f.cancel(false);
        }
    }

    @Nullable
    private static ScheduledFuture<?> scheduleReadTimeout(@Nullable final ReadTimeoutTask readTimeoutTask, final long timeoutMillis) {
        if (timeoutMillis == NO_SCHEDULE_TIME) {
            return null;
        } else {
            return assertNotNull(readTimeoutTask).schedule(timeoutMillis);
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
            } catch (Throwable t) {
                ctx.fireExceptionCaught(t);
            }
        }

        @Nullable
        private ScheduledFuture<?> schedule(final long timeoutMillis) {
            return timeoutMillis > 0 ? ctx.executor().schedule(this, timeoutMillis, MILLISECONDS) : null;
        }
    }
}
