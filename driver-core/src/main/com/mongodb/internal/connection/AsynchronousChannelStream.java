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
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.Stream;
import org.bson.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class AsynchronousChannelStream implements Stream {
    private final ServerAddress serverAddress;
    private final SocketSettings settings;
    private final PowerOfTwoBufferPool bufferProvider;
    private volatile ExtendedAsynchronousByteChannel channel;
    private volatile boolean isClosed;

    public AsynchronousChannelStream(final ServerAddress serverAddress, final SocketSettings settings,
                                     final PowerOfTwoBufferPool bufferProvider) {
        this.serverAddress = serverAddress;
        this.settings = settings;
        this.bufferProvider = bufferProvider;
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

    public synchronized ExtendedAsynchronousByteChannel getChannel() {
        return channel;
    }

    protected synchronized void setChannel(final ExtendedAsynchronousByteChannel channel) {
        isTrue("current channel is null", this.channel == null);
        if (isClosed) {
            closeChannel(channel);
        } else {
            this.channel = channel;
        }
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        AsyncWritableByteChannelAdapter byteChannel = new AsyncWritableByteChannelAdapter();
        Iterator<ByteBuf> iter = buffers.iterator();
        pipeOneBuffer(byteChannel, iter.next(), new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void t) {
                if (iter.hasNext()) {
                    pipeOneBuffer(byteChannel, iter.next(), this);
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
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        readAsync(numBytes, 0, handler);
    }

    private void readAsync(final int numBytes, final int additionalTimeout, final AsyncCompletionHandler<ByteBuf> handler) {
        ByteBuf buffer = bufferProvider.getBuffer(numBytes);

        int timeout = settings.getReadTimeout(MILLISECONDS);
        if (timeout > 0 && additionalTimeout > 0) {
            timeout += additionalTimeout;
        }

        channel.read(buffer.asNIO(), timeout, MILLISECONDS, null, new BasicCompletionHandler(buffer, handler));
    }

    @Override
    public void open() throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<Void>();
        openAsync(handler);
        handler.getOpen();
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<Void>();
        writeAsync(buffers, handler);
        handler.getWrite();
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        FutureAsyncCompletionHandler<ByteBuf> handler = new FutureAsyncCompletionHandler<ByteBuf>();
        readAsync(numBytes, handler);
        return handler.getRead();
    }

    @Override
    public boolean supportsAdditionalTimeout() {
        return true;
    }

    @Override
    public ByteBuf read(final int numBytes, final int additionalTimeout) throws IOException {
        FutureAsyncCompletionHandler<ByteBuf> handler = new FutureAsyncCompletionHandler<ByteBuf>();
        readAsync(numBytes, additionalTimeout, handler);
        return handler.getRead();
    }

    @Override
    public ServerAddress getAddress() {
        return serverAddress;
    }

    @Override
    public synchronized void close() {
        isClosed = true;
        try {
            closeChannel(channel);
        } finally {
            channel = null;
        }
    }

    private void closeChannel(final ExtendedAsynchronousByteChannel channel) {
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
                               final AsyncCompletionHandler<Void> outerHandler) {
        byteChannel.write(byteBuffer.asNIO(), new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void t) {
                if (byteBuffer.hasRemaining()) {
                    byteChannel.write(byteBuffer.asNIO(), this);
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
        void write(final ByteBuffer src, final AsyncCompletionHandler<Void> handler) {
            channel.write(src, null, new AsyncWritableByteChannelAdapter.WriteCompletionHandler(handler));
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
            public void failed(final Throwable exc, final Object attachment) {
                AsyncCompletionHandler<Void> localHandler = getHandlerAndClear();
                localHandler.failed(exc);
            }
        }
    }

    private final class BasicCompletionHandler extends BaseCompletionHandler<ByteBuf, Integer, Void> {
        private final AtomicReference<ByteBuf> byteBufReference;

        private BasicCompletionHandler(final ByteBuf dst, final AsyncCompletionHandler<ByteBuf> handler) {
            super(handler);
            this.byteBufReference = new AtomicReference<ByteBuf>(dst);
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
                channel.read(localByteBuf.asNIO(), settings.getReadTimeout(MILLISECONDS), MILLISECONDS, null,
                        new BasicCompletionHandler(localByteBuf, localHandler));
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
            this.handlerReference = new AtomicReference<AsyncCompletionHandler<T>>(handler);
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
        public void completed(final T result) {
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
                throw new MongoInterruptedException(prefix + " the AsynchronousSocketChannelStream failed", e);

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
