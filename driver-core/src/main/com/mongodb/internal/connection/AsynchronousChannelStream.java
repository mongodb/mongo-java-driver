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

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.lang.Nullable;
import org.bson.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class AsynchronousChannelStream implements Stream {
    private final ServerAddress serverAddress;
    private final SocketSettings settings;
    private final PowerOfTwoBufferPool bufferProvider;
    // we use `AtomicReference` to guarantee that we do not call `ExtendedAsynchronousByteChannel.close` concurrently with itself
    private final AtomicReference<ExtendedAsynchronousByteChannel> channel;
    private volatile boolean isClosed;

    public AsynchronousChannelStream(final ServerAddress serverAddress, final SocketSettings settings,
                                     final PowerOfTwoBufferPool bufferProvider) {
        this.serverAddress = serverAddress;
        this.settings = settings;
        this.bufferProvider = bufferProvider;
        channel = new AtomicReference<>();
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public SocketSettings getSettings() {
        return settings;
    }

    public PowerOfTwoBufferPool getBufferProvider() {
        return bufferProvider;
    }

    public ExtendedAsynchronousByteChannel getChannel() {
        return channel.get();
    }

    protected void setChannel(final ExtendedAsynchronousByteChannel channel) {
        if (isClosed) {
            closeChannel(channel);
        } else {
            assertTrue(this.channel.compareAndSet(null, channel));
            if (isClosed) {
                closeChannel(this.channel.getAndSet(null));
            }
        }
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final OperationContext operationContext,
            final AsyncCompletionHandler<Void> handler) {
        AsyncWritableByteChannelAdapter byteChannel = new AsyncWritableByteChannelAdapter();
        Iterator<ByteBuf> iter = buffers.iterator();
        pipeOneBuffer(byteChannel, iter.next(), operationContext, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(@Nullable final Void t) {
                if (iter.hasNext()) {
                    pipeOneBuffer(byteChannel, iter.next(), operationContext, this);
                } else {
                    handler.completed(null);
                }
            }

            @Override
            public void failed(final Throwable t) {
                handler.failed(t);
            }
        });
    }

    @Override
    public void readAsync(final int numBytes, final OperationContext operationContext, final AsyncCompletionHandler<ByteBuf> handler) {
        readAsync(numBytes, 0, operationContext, handler);
    }

    private void readAsync(final int numBytes, final int additionalTimeout, final OperationContext operationContext,
            final AsyncCompletionHandler<ByteBuf> handler) {
        ByteBuf buffer = bufferProvider.getBuffer(numBytes);

        long timeout = operationContext.getTimeoutContext().getReadTimeoutMS();
        if (timeout > 0 && additionalTimeout > 0) {
            timeout += additionalTimeout;
        }
        getChannel().read(buffer.asNIO(), timeout, MILLISECONDS, null, new BasicCompletionHandler(buffer, operationContext, handler));
    }

    @Override
    public void open(final OperationContext operationContext) throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<>();
        openAsync(operationContext, handler);
        handler.getOpen();
    }

    @Override
    public void write(final List<ByteBuf> buffers, final OperationContext operationContext) throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<>();
        writeAsync(buffers, operationContext, handler);
        handler.getWrite();
    }

    @Override
    public ByteBuf read(final int numBytes, final OperationContext operationContext) throws IOException {
        FutureAsyncCompletionHandler<ByteBuf> handler = new FutureAsyncCompletionHandler<>();
        readAsync(numBytes, operationContext, handler);
        return handler.getRead();
    }

    @Override
    public ByteBuf read(final int numBytes, final int additionalTimeout, final OperationContext operationContext) throws IOException {
        FutureAsyncCompletionHandler<ByteBuf> handler = new FutureAsyncCompletionHandler<>();
        readAsync(numBytes, additionalTimeout, operationContext, handler);
        return handler.getRead();
    }

    @Override
    public ServerAddress getAddress() {
        return serverAddress;
    }

    @Override
    public void close() {
        isClosed = true;
        closeChannel(this.channel.getAndSet(null));
    }

    private void closeChannel(@Nullable final ExtendedAsynchronousByteChannel channel) {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return bufferProvider.getBuffer(size);
    }

    private void pipeOneBuffer(final AsyncWritableByteChannelAdapter byteChannel, final ByteBuf byteBuffer,
            final OperationContext operationContext, final AsyncCompletionHandler<Void> outerHandler) {
        byteChannel.write(byteBuffer.asNIO(), operationContext, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(@Nullable final Void t) {
                if (byteBuffer.hasRemaining()) {
                    byteChannel.write(byteBuffer.asNIO(), operationContext, this);
                } else {
                    outerHandler.completed(null);
                }
            }

            @Override
            public void failed(final Throwable t) {
                outerHandler.failed(t);
            }
        });
    }

    private class AsyncWritableByteChannelAdapter {
        void write(final ByteBuffer src, final OperationContext operationContext, final AsyncCompletionHandler<Void> handler) {
            getChannel().write(src, operationContext.getTimeoutContext().getWriteTimeoutMS(), MILLISECONDS, null,
                    new AsyncWritableByteChannelAdapter.WriteCompletionHandler(handler));
        }

        private class WriteCompletionHandler extends BaseCompletionHandler<Void, Integer, Object> {

            WriteCompletionHandler(final AsyncCompletionHandler<Void> handler) {
                super(handler);
            }

            @Override
            public void completed(final Integer result, final Object attachment) {
                AsyncCompletionHandler<Void> localHandler = getHandlerAndClear();
                localHandler.completed(null);
            }

            @Override
            public void failed(final Throwable t, final Object attachment) {
                AsyncCompletionHandler<Void> localHandler = getHandlerAndClear();
                if (t instanceof InterruptedByTimeoutException) {
                    localHandler.failed(new MongoSocketWriteTimeoutException("Timeout while writing message", serverAddress, t));
                } else {
                    localHandler.failed(t);
                }
            }
        }
    }

    private final class BasicCompletionHandler extends BaseCompletionHandler<ByteBuf, Integer, Void> {
        private final AtomicReference<ByteBuf> byteBufReference;
        private final OperationContext operationContext;

        private BasicCompletionHandler(final ByteBuf dst, final OperationContext operationContext,
                final AsyncCompletionHandler<ByteBuf> handler) {
            super(handler);
            this.byteBufReference = new AtomicReference<>(dst);
            this.operationContext = operationContext;
        }

        @Override
        public void completed(final Integer result, final Void attachment) {
            AsyncCompletionHandler<ByteBuf> localHandler = getHandlerAndClear();
            ByteBuf localByteBuf = byteBufReference.getAndSet(null);
            if (result == -1) {
                localByteBuf.release();
                localHandler.failed(new MongoSocketReadException("Prematurely reached end of stream", serverAddress));
            } else if (!localByteBuf.hasRemaining()) {
                localByteBuf.flip();
                localHandler.completed(localByteBuf);
            } else {
                getChannel().read(localByteBuf.asNIO(), operationContext.getTimeoutContext().getReadTimeoutMS(), MILLISECONDS, null,
                        new BasicCompletionHandler(localByteBuf, operationContext, localHandler));
            }
        }

        @Override
        public void failed(final Throwable t, final Void attachment) {
            AsyncCompletionHandler<ByteBuf> localHandler = getHandlerAndClear();
            ByteBuf localByteBuf = byteBufReference.getAndSet(null);
            localByteBuf.release();
            if (t instanceof InterruptedByTimeoutException) {
                localHandler.failed(new MongoSocketReadTimeoutException("Timeout while receiving message", serverAddress, t));
            } else {
                localHandler.failed(t);
            }
        }
    }

    // Private base class for all CompletionHandler implementors that ensures the upstream handler is
    // set to null before it is used.  This is to work around an observed issue with implementations of
    // AsynchronousSocketChannel that fail to clear references to handlers stored in instance fields of
    // the class.
    private abstract static class BaseCompletionHandler<T, V, A> implements CompletionHandler<V, A> {
        private final AtomicReference<AsyncCompletionHandler<T>> handlerReference;

        BaseCompletionHandler(final AsyncCompletionHandler<T> handler) {
            this.handlerReference = new AtomicReference<>(handler);
        }

        AsyncCompletionHandler<T> getHandlerAndClear() {
            return handlerReference.getAndSet(null);
        }
    }

    static class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile T result;
        private volatile Throwable error;

        @Override
        public void completed(@Nullable final T result) {
            this.result = result;
            latch.countDown();
        }

        @Override
        public void failed(final Throwable t) {
            this.error = t;
            latch.countDown();
        }

        void getOpen() throws IOException {
            get("Opening");
        }

        void getWrite() throws IOException {
            get("Writing to");
        }

        T getRead() throws IOException {
            return get("Reading from");
        }

        private T get(final String prefix) throws IOException {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw interruptAndCreateMongoInterruptedException(prefix + " the AsynchronousSocketChannelStream failed", e);

            }
            if (error != null) {
                if (error instanceof IOException) {
                    throw (IOException) error;
                } else if (error instanceof MongoException) {
                    throw (MongoException) error;
                } else {
                    throw new MongoInternalException(prefix + " the TlsChannelStream failed", error);
                }
            }
            return result;
        }

    }
}
