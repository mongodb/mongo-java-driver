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

package com.mongodb.internal.connection.grpc;

import com.mongodb.Function;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.Stream;
import com.mongodb.internal.time.Timeout;
import com.mongodb.internal.connection.grpc.GrpcStream.WriteState.PendingWrite;
import com.mongodb.internal.connection.grpc.GrpcStreamFactory.ClientMetadataDocument;
import com.mongodb.internal.connection.grpc.GrpcStreamFactory.NettyByteBufProvider;
import com.mongodb.internal.connection.netty.NettyByteBuf;
import com.mongodb.lang.Nullable;
import io.grpc.ClientCall;
import io.grpc.Detachable;
import io.grpc.KnownLength;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.bson.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

/**
 * A {@link Stream} implemented via
 * <a href="https://grpc.io/docs/what-is-grpc/core-concepts/#bidirectional-streaming-rpc">gRPC bidirectional streaming</a>.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.</p>
 */
// While `GrpcStream` does not have to be thread-safe, some of its state may still be accessed concurrently by a thread
// calling a method on an instance of `GrpcStream`, and an internal Java gRPC thread.
// For example, `GrpcStream` may be closed not only by the rest of the driver but also by Java gRPC.
// The following is a pattern used in this implementation to deal with conflicts arising from the aforementioned concurrency:
// 1) Do actions that may conflict with a concurrent closing activity.
// 2) Check if the closing activity has started.
// 2.1) If "no", then there is nothing to worry about as now we know that actions in 1) are ordered before (in the happens-before order)
//      the closing activity, and the closing activity will fully see the results of 1) and act accordingly.
// 2.2) If "yes", then the closing activity is concurrent with 1) and may not fully see the results of 1).
//      Start the closing activity to guarantee that it fully sees the results of 1).
//      Note that this requires the closing activity to support being run concurrently with itself.
@NotThreadSafe
final class GrpcStream implements Stream {
    private static final String SERVICE_NAME = "mongodb.CommandService";
    private static final String UNAUTH_STREAM_FULL_METHOD_NAME =
            MethodDescriptor.generateFullMethodName(SERVICE_NAME, "UnauthenticatedCommandStream");
    private static final String AUTH_STREAM_FULL_METHOD_NAME =
            MethodDescriptor.generateFullMethodName(SERVICE_NAME, "AuthenticatedCommandStream");
    private static final Marshallers<PendingWrite, InputStream> MARSHALLERS = new Marshallers<>(
            new PendingWriteMarshaller(), new InputStreamUnmarshaller());
    private static final int INFINITE_SOCKET_READ_TIMEOUT = 0;

    /**
     * {@value #END_OF_STREAM}.
     */
    static final int END_OF_STREAM = -1;

    private final ServerAddress serverAddress;
    private final UUID clientId;
    @Nullable
    private final ClientMetadataDocument clientMetadataDocument;
    private final SocketSettings socketSettings;
    private final NettyByteBufProvider bufferProvider;
    private final WriteState writeState;
    private final ReadState readState;
    private final ClientCall<PendingWrite, InputStream> call;
    private final AtomicBoolean closed;

    GrpcStream(
            final ServerAddress serverAddress,
            final UUID clientId,
            @Nullable
            final ClientMetadataDocument clientMetadataDocument,
            final SocketSettings socketSettings,
            final NettyByteBufProvider bufferProvider,
            final BiFunction<String, Marshallers<PendingWrite, InputStream>, ClientCall<PendingWrite, InputStream>> callCreator) {
        this.serverAddress = serverAddress;
        this.clientId = clientId;
        this.clientMetadataDocument = clientMetadataDocument;
        this.socketSettings = socketSettings;
        this.bufferProvider = bufferProvider;
        Exceptions exceptions = new Exceptions();
        this.writeState = new WriteState(exceptions);
        this.readState = new ReadState(exceptions);
        this.call = callCreator.apply(UNAUTH_STREAM_FULL_METHOD_NAME, MARSHALLERS);
        closed = new AtomicBoolean();
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return bufferProvider.getBuffer(size);
    }

    @Override
    public void open() {
        assertFalse(closed.get());
        Metadata metadata = new Metadata();
        // if we set a value for the `Content-Type` key, Java gRPC overrides the value back to `application/grpc`, so we are not setting it
        metadata.put(Metadata.Key.of("mongodb-clientId", ASCII_STRING_MARSHALLER), clientId.toString());
        if (clientMetadataDocument != null) {
            metadata.put(Metadata.Key.of("mongodb-client", ASCII_STRING_MARSHALLER), clientMetadataDocument.base64());
        }
        // VAKOTODO what value to use for mongodb-wireVersion?
        metadata.put(Metadata.Key.of("mongodb-wireVersion", ASCII_STRING_MARSHALLER), "18");
        configureJunkMetadata(metadata);
        // VAKOTODO use `CallOptions.withDeadlineAfter` when calling `ManagedChannel.newCall` in `GrpcStreamFactory`?
        // See https://grpc.io/docs/guides/deadlines/ and https://grpc.io/blog/deadlines/,
        // don't forget to handle the DEADLINE_EXCEEDED status.
        call.start(new ClientCall.Listener<InputStream>() {
            @Override
            public void onHeaders(final Metadata metadata) {
                // we do not expect any useful metadata
            }

            @Override
            public void onMessage(
                    // We must close `response`. Java gRPC does not do that
                    // because for Java gRPC this is just a message we created in `Marshaller.parse`,
                    // which makes us responsible for its lifecycle.
                    final InputStream response) {
                readState.add(response);
            }

            @Override
            public void onReady() {
                // There seem to be no reason for us to implement this callback.
                // Java gRPC says "Calls that send exactly one message should not await this callback".
                // While we may sometimes send more than one message
                // (for example, https://www.mongodb.com/docs/current/reference/command/getMore/),
                // we still wait for a server response after each one.
            }

            @Override
            public void onClose(final Status status, final Metadata metadata) {
                StatusRuntimeException statusException;
                if (status == Status.OK || status == Status.CANCELLED) {
                    assertNull(status.getCause());
                    statusException = null;
                } else {
                    statusException = status.asRuntimeException(metadata);
                }
                close(statusException, true);
            }
        }, metadata);
        // We do not care about flow control because a server usually replies with a single message.
        // The server will not flood up with messages even if there are `exhaustAllowed`/`moreToCome` `OP_MSG` flags
        // (https://www.mongodb.com/docs/upcoming/reference/mongodb-wire-protocol/#op_msg).
        call.request(Integer.MAX_VALUE);
    }

    @Override
    public void openAsync(final AsyncCompletionHandler<Void> handler) {
        fail("VAKOTODO");
    }

    /**
     * @param buffers {@inheritDoc} {@code buffers} must contain exactly one
     * <a href="https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/">MongoDB Wire Protocol</a> message,
     * which is not a usual requirement for {@link Stream#write(List)}.
     */
    @Override
    public void write(final List<ByteBuf> buffers) {
        writeState.startWrite(call, buffers).blockInterruptiblyUntilCompleted();
    }

    @Override
    public ByteBuf read(final int numBytes) {
        return read(numBytes, 0);
    }

    @Override
    public boolean supportsAdditionalTimeout() {
        return true;
    }

    @Override
    public ByteBuf read(final int numBytes, final int additionalTimeout) {
        Timeout timeout = startNow(additionalTimeout);
        ReadState.PendingRead pendingRead;
        io.netty.buffer.ByteBuf buffer = bufferProvider.allocator().buffer(numBytes, numBytes);
        try {
            buffer.touch();
            pendingRead = readState.startRead(numBytes, buffer, timeout);
        } finally {
            buffer.release();
        }
        return pendingRead.blockInterruptiblyUntilCompleted();
    }

    /**
     * @param buffers {@inheritDoc} {@code buffers} must contain exactly one
     * <a href="https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/">MongoDB Wire Protocol</a> message,
     * which is not a usual requirement for {@link Stream#writeAsync(List, AsyncCompletionHandler)}.
     */
    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        fail("VAKOTODO");
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        fail("VAKOTODO");
    }

    @Override
    public ServerAddress getAddress() {
        return serverAddress;
    }

    @Override
    @SuppressWarnings("try")
    public void close() {
        close(null, false);
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public String toString() {
        return "GrpcStream{"
                + "serverAddress=" + serverAddress
                + ", clientId=" + clientId
                + '}';
    }

    // VAKOTODO this is junk required by https://github.com/10gen/atlasproxy, delete in the future.
    private void configureJunkMetadata(final Metadata metadata) {
        // VAKOTODO is this even needed?
        metadata.put(Metadata.Key.of("Username", ASCII_STRING_MARSHALLER), "user");
        metadata.put(Metadata.Key.of("ServerName", ASCII_STRING_MARSHALLER), "host.local.10gen.cc");
        // This is for the `removeConnectionId` command and for `UnauthenticatedCommandStream`
        // See https://docs.google.com/document/d/1ozvF6TTjRpl1g8alx0joOJ5X9elYDnM7UfTKTki8AfA/edit#bookmark=id.5tnw539alz3.
        metadata.put(Metadata.Key.of("security-uuid", ASCII_STRING_MARSHALLER), clientId.toString());
        metadata.put(Metadata.Key.of("x-forwarded-for", ASCII_STRING_MARSHALLER), "127.0.0.1:9901");
    }

    /**
     * This method may be called concurrently with itself because Java gRPC may call it at any point,
     * potentially concurrently with it being called by the rest of the driver via {@link GrpcStream#close()}.
     * <p>
     * Idempotent.</p>
     *
     * @param fromClientCallListener {@code true} iff called from {@link ClientCall.Listener#onClose(Status, Metadata)}.
     */
    @SuppressWarnings("try")
    private void close(@Nullable final StatusRuntimeException callStatusException, final boolean fromClientCallListener) {
        if (closed.compareAndSet(false, true)) {
            try (NoCheckedAutoCloseable writeState = () -> this.writeState.close(callStatusException);
                 NoCheckedAutoCloseable readState = () -> this.readState.close(callStatusException)) {
                if (!fromClientCallListener) {
                    // At this point we know that we were called by the rest of the driver via the `Stream.close` method,
                    // rather than by Java gRPC. Therefore, we are not running concurrently with any `ClientCall` methods,
                    // and are allowed to run `ClientCall.cancel` despite it not being thread-safe.
                    call.cancel(this + " was closed", assertNull(callStatusException));
                }
            }
        }
    }

    private Timeout startNow(final int additionalTimeout) {
        int socketReadTimeoutMillis = socketSettings.getReadTimeout(TimeUnit.MILLISECONDS);
        return socketReadTimeoutMillis == INFINITE_SOCKET_READ_TIMEOUT
                ? Timeout.infinite()
                : Timeout.startNow(socketReadTimeoutMillis + additionalTimeout, TimeUnit.MILLISECONDS);
    }

    private static final class ResourceUtil {
        static <T extends Iterable<? extends ByteBuf>> T retain(final T buffers) {
            // we assume `ByteBuf::retain` does not complete abruptly
            buffers.forEach(ByteBuf::retain);
            return buffers;
        }

        static void release(final Iterable<? extends ByteBuf> buffers) {
            // we assume `ByteBuf::release` does not complete abruptly
            buffers.forEach(buffer -> {
                if (buffer != null) {
                    buffer.release();
                }
            });
        }

        static void close(final Iterable<? extends AutoCloseable> autoCloseables) {
            Function<Exception, RuntimeException> exceptionTransformer = e -> {
                if (e instanceof RuntimeException) {
                    return (RuntimeException) e;
                }
                return new RuntimeException(e);
            };
            RuntimeException mainException = null;
            for (AutoCloseable autoCloseable : autoCloseables) {
                try {
                    autoCloseable.close();
                } catch (Exception e) {
                    if (mainException == null) {
                        mainException = exceptionTransformer.apply(e);
                    } else {
                        mainException.addSuppressed(e);
                    }
                }
            }
            if (mainException != null) {
                throw mainException;
            }
        }

        private ResourceUtil() {
            fail();
        }
    }

    @FunctionalInterface
    private interface NoCheckedAutoCloseable extends AutoCloseable {
        void close();
    }

    @ThreadSafe
    static final class Marshallers<ReqT, RespT> {
        private final Marshaller<ReqT> marshaller;
        private final Marshaller<RespT> unmarshaller;

        Marshallers(
                final Marshaller<ReqT> marshaller,
                final Marshaller<RespT> unmarshaller) {
            this.marshaller = marshaller;
            this.unmarshaller = unmarshaller;
        }

        Marshaller<ReqT> marshaller() {
            return marshaller;
        }

        Marshaller<RespT> unmarshaller() {
            return unmarshaller;
        }
    }

    private final class Exceptions {
        Exceptions() {
        }

        MongoSocketClosedException writeFailedStreamClosed() {
            return new MongoSocketClosedException("Cannot write to a closed stream", serverAddress);
        }

        MongoSocketClosedException readFailedStreamClosed() {
            return new MongoSocketClosedException("Cannot read from a closed stream", serverAddress);
        }

        MongoSocketWriteException writeFailed(final Throwable cause) {
            return new MongoSocketWriteException("Exception sending message", serverAddress, assertNotNull(cause));
        }

        MongoSocketReadException readFailed(final Throwable cause) {
            return new MongoSocketReadException("Exception receiving message", serverAddress, assertNotNull(cause));
        }

        MongoSocketReadTimeoutException readTimedOut() {
            return new MongoSocketReadTimeoutException("Timeout while receiving message", serverAddress, null);
        }

        MongoInterruptedException interrupted(@Nullable final InterruptedException e) {
            return new MongoInterruptedException(null, e);
        }
    }

    @ThreadSafe
    static final class WriteState {
        private final Exceptions exceptions;
        /**
         * {@code null} only if {@link #startWrite(ClientCall, List)} has not been called.
         */
        @Nullable
        private volatile PendingWrite pendingWrite;
        private volatile boolean closed;

        WriteState(final Exceptions exceptions) {
            this.exceptions = exceptions;
            closed = false;
        }

        /**
         * Must not be called if there is another {@link PendingWrite} that has not been {@linkplain PendingWrite#isCompleted() completed}.
         *
         * @param message Retained until {@link PendingWrite} is {@linkplain PendingWrite#isCompleted() completed}.
         */
        PendingWrite startWrite(final ClientCall<PendingWrite, InputStream> call, final List<ByteBuf> message) {
            PendingWrite pendingWrite = this.pendingWrite;
            if (pendingWrite != null) {
                assertTrue(pendingWrite.isCompleted());
            }
            pendingWrite = new PendingWrite(message, exceptions);
            this.pendingWrite = pendingWrite;
            if (closed) {
                closePendingWrite(pendingWrite, null);
                return pendingWrite;
            }
            try {
                // `ClientCall.sendMessage` may not call `Marshaller.stream`
                // using call stack, but rather store the message (`pendingWrite`) in heap memory,
                // so that later (in the happens-before order) `Marshaller.stream` could access it.
                // `Marshaller.stream` may never access and complete `pendingWrite`,
                // but that is fine because we also store a reference to `pendingWrite` in heap memory,
                // and we guarantee that eventually it will be complete either by us or by `Marshaller.stream`.
                //
                // Note that due to the contract of the `startWrite` method,
                // `call.sendMessage` is guaranteed to be called sequentially. We must guarantee this
                // because `sendMessage` is not thread-safe.
                call.sendMessage(pendingWrite);
            } catch (Exception e) {
                pendingWrite.completeExceptionally(e);
            }
            return pendingWrite;
        }

        /**
         * Must be called only from {@link GrpcStream#close(StatusRuntimeException, boolean)},
         * which ensures that it is called not more than once.
         * <p>
         * This method cannot be called more than once, but {@link #pendingWrite} may be written to concurrently with this method
         * executing, because {@link GrpcStream#close(StatusRuntimeException, boolean)} may be called by Java gRPC at any point.
         * We must make sure that {@link WriteState}
         * cannot end up in a closed state with {@link #pendingWrite} not being {@linkplain PendingWrite#isCompleted() completed}.</p>
         */
        void close(@Nullable final StatusRuntimeException callStatusException) {
            assertFalse(closed);
            closed = true;
            closePendingWrite(this.pendingWrite, callStatusException);
        }

        private void closePendingWrite(@Nullable final PendingWrite pendingWrite, @Nullable final StatusRuntimeException callStatusException) {
            if (pendingWrite != null) {
                pendingWrite.completeExceptionally(callStatusException == null
                        ? exceptions.writeFailedStreamClosed()
                        : callStatusException);
            }
        }

        @ThreadSafe
        static final class PendingWrite {
            /**
             * Contains {@code null} only if {@link #message} is {@linkplain #detachMessage() detached},
             * or {@link PendingWrite} is {@linkplain #isCompleted() completed}.
             */
            private final AtomicReference<List<ByteBuf>> message;
            private final Exceptions exceptions;
            private final CompletableFuture<Void> future;

            private PendingWrite(final List<ByteBuf> message, final Exceptions exceptions) {
                this.message = new AtomicReference<>(ResourceUtil.retain(message));
                this.exceptions = exceptions;
                this.future = new CompletableFuture<>();
            }

            /**
             * A caller is responsible for {@linkplain InputStream#close() closing} the {@link InputStream}.
             * Must not be called more than once.
             */
            InputStream detachedMessageInputStream() {
                return new DetachedMessageInputStream();
            }

            void blockInterruptiblyUntilCompleted() throws MongoInterruptedException {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw exceptions.interrupted(e);
                } catch (ExecutionException e) {
                    throw exceptions.writeFailed(assertNotNull(e.getCause()));
                }
            }

            boolean isCompleted() {
                return future.isDone();
            }

            void completeExceptionally(final Exception cause) {
                try {
                    future.completeExceptionally(cause);
                } finally {
                    detachAndReleaseMessage();
                }
            }

            void complete() {
                try {
                    future.complete(null);
                } finally {
                    detachAndReleaseMessage();
                }
            }

            private void detachAndReleaseMessage() {
                List<ByteBuf> detachedMessage = this.message.getAndSet(null);
                if (detachedMessage != null) {
                    ResourceUtil.release(detachedMessage);
                }
            }

            /**
             * A caller becomes responsible for {@linkplain ByteBuf#release() releasing} the buffers.
             * Must not be called more than once.
             */
            private List<ByteBuf> detachMessage() {
                List<ByteBuf> detachedMessage = this.message.getAndSet(null);
                if (detachedMessage == null) {
                    // this `PendingWrite` was completed exceptionally
                    assertTrue(future.isCompletedExceptionally());
                    // `join` to propagate the exception
                    future.join();
                    throw fail();
                }
                return detachedMessage;
            }

            final class DetachedMessageInputStream extends InputStream implements KnownLength {
                /**
                 * {@code null} iff {@linkplain #close() closed}.
                 */
                @Nullable
                private ArrayDeque<ByteBuf> buffers;
                private int available;

                private DetachedMessageInputStream() {
                    this.buffers = new ArrayDeque<>(detachMessage());
                    available = buffers.stream()
                            .filter(Objects::nonNull)
                            .mapToInt(ByteBuf::remaining)
                            .sum();
                }

                @Override
                public int read() throws IOException {
                    throwIfClosed();
                    ArrayDeque<ByteBuf> buffers = assertNotNull(this.buffers);
                    int bufferRemaining = 0;
                    int result = END_OF_STREAM;
                    ByteBuf buffer = buffers.peek();
                    while (buffer != null) {
                        bufferRemaining = buffer.remaining();
                        if (bufferRemaining == 0) {
                            buffers.remove().release();
                            buffer = buffers.peek();
                        } else {
                            result = buffer.get();
                            bufferRemaining--;
                            available--;
                            break;
                        }
                    }
                    if (bufferRemaining == 0 && buffer != null) {
                        buffers.remove().release();
                    }
                    return result;
                }

                @Override
                public int read(final byte[] target, final int off, final int len) throws IOException {
                    if (off < 0 || len < 0 || target.length - off < len) {
                        // `InputStream` requires `IllegalArgumentException`
                        throw new IllegalArgumentException(String.format("off=%d, len=%d, target.length=%d", off, len, target.length));
                    }
                    throwIfClosed();
                    if (len == 0) {
                        return 0;
                    }
                    ArrayDeque<ByteBuf> buffers = assertNotNull(this.buffers);
                    int bufferRemaining = 0;
                    int unreadLen = len;
                    ByteBuf buffer = buffers.peek();
                    while (buffer != null) {
                        bufferRemaining = buffer.remaining();
                        if (bufferRemaining < unreadLen) {
                            buffer.get(target, off, bufferRemaining);
                            unreadLen -= bufferRemaining;
                            bufferRemaining = 0;
                            buffers.remove().release();
                            buffer = buffers.peek();
                        } else {
                            buffer.get(target, off, unreadLen);
                            bufferRemaining -= unreadLen;
                            unreadLen = 0;
                            break;
                        }
                    }
                    if (bufferRemaining == 0 && buffer != null) {
                        buffers.remove().release();
                    }
                    int readLen = len - unreadLen;
                    available -= readLen;
                    return readLen > 0 ? readLen : END_OF_STREAM;
                }

                @Override
                public int available() throws IOException {
                    throwIfClosed();
                    return available;
                }

                @Override
                @SuppressWarnings("try")
                public void close() {
                    ArrayDeque<ByteBuf> buffers = this.buffers;
                    if (buffers != null) {
                        try {
                            completePendingWrite();
                        } finally {
                            ResourceUtil.release(buffers);
                            buffers.clear();
                            this.buffers = null;
                        }
                    }
                }

                private void completePendingWrite() {
                    if (available > 0) {
                        completeExceptionally(new RuntimeException("Message was not fully sent by Java gRPC"));
                    } else {
                        complete();
                    }
                }

                private void throwIfClosed() throws IOException {
                    if (buffers == null) {
                        // `InputStream` requires `IOException`
                        throw new IOException(DetachedMessageInputStream.class.getSimpleName() + " is closed");
                    }
                }
            }
        }
    }

    private static final class ReadState {
        private final Exceptions exceptions;
        // I expect `LinkedBlockingQueue` adding smaller latency overhead than `ConcurrentLinkedQueue` when used here
        // (the latter appears to be quite bad at the very least when there is not much contention).
        // `ArrayBlockingQueue` would have been much better than both, but it is bounded, which is not suitable here.
        private final LinkedBlockingQueue<InputStream> inputStreams;
        /**
         * {@code null} only if {@link #startRead(int, io.netty.buffer.ByteBuf, Timeout)} has not been called.
         */
        @Nullable
        private volatile PendingRead pendingRead;
        private volatile boolean closed;

        ReadState(final Exceptions exceptions) {
            this.exceptions = exceptions;
            inputStreams = new LinkedBlockingQueue<>();
            closed = false;
        }

        /**
         * Must not be called if there is another {@link PendingRead} that has not been {@linkplain PendingRead#isCompleted() completed}.
         *
         * @param message Retained until {@link PendingRead} is {@linkplain PendingRead#isCompleted() completed}.
         */
        PendingRead startRead(final int numBytes, final io.netty.buffer.ByteBuf message, final Timeout timeout) {
            PendingRead pendingRead = this.pendingRead;
            if (pendingRead != null) {
                assertTrue(pendingRead.isCompleted());
            }
            pendingRead = new PendingRead(numBytes, message, timeout, inputStreams, exceptions);
            this.pendingRead = pendingRead;
            if (closed) {
                closePendingRead(pendingRead, null);
                return pendingRead;
            }
            pendingRead.tryComplete();
            return pendingRead;
        }

        void add(final InputStream inputStream) {
            this.inputStreams.add(inputStream);
            if (closed) {
                clearAndClose(inputStreams);
                return;
            }
            PendingRead pendingRead = this.pendingRead;
            if (pendingRead != null) {
                pendingRead.tryComplete();
            }
        }

        /**
         * Must be called only from {@link GrpcStream#close(StatusRuntimeException, boolean)},
         * which ensures that it is called not more than once.
         * <p>
         * This method cannot be called more than once, but {@link #pendingRead} may be written to concurrently with this method
         * executing, because {@link GrpcStream#close(StatusRuntimeException, boolean)} may be called by Java gRPC at any point.
         * We must make sure that {@link ReadState}
         * cannot end up in a closed state with {@link #pendingRead} not being {@linkplain PendingRead#isCompleted() completed}.</p>
         */
        void close(@Nullable final StatusRuntimeException callStatusException) {
            assertFalse(closed);
            closed = true;
            try {
                closePendingRead(this.pendingRead, callStatusException);
            } finally {
                clearAndClose(inputStreams);
            }
        }

        private void closePendingRead(@Nullable final PendingRead pendingRead, @Nullable final StatusRuntimeException callStatusException) {
            if (pendingRead != null) {
                pendingRead.completeExceptionally(callStatusException == null
                        ? exceptions.readFailedStreamClosed()
                        : callStatusException);
            }
        }

        private void clearAndClose(final LinkedBlockingQueue<InputStream> inputStreams) {
            ArrayList<AutoCloseable> autoCloseables = new ArrayList<>();
            inputStreams.drainTo(autoCloseables);
            ResourceUtil.close(autoCloseables);
        }

        @ThreadSafe
        static final class PendingRead {
            /**
             * Must be guarded with {@link #messageLock}.
             */
            private int unreadNumBytes;
            /**
             * Contains {@code null} only if {@link PendingRead} is completed or is being tried to be completed.
             */
            private final AtomicReference<io.netty.buffer.ByteBuf> message;
            private final Lock messageLock;
            private final Timeout timeout;
            /**
             * Stays not {@linkplain ResourceUtil#close(Iterable) closed} when {@link PendingWrite} is {@linkplain #isCompleted() completed}.
             */
            private final LinkedBlockingQueue<InputStream> inputStreams;
            private final Exceptions exceptions;
            private final CompletableFuture<ByteBuf> future;

            private PendingRead(
                    final int numBytes,
                    final io.netty.buffer.ByteBuf message,
                    final Timeout timeout,
                    final LinkedBlockingQueue<InputStream> inputStreams,
                    final Exceptions exceptions) {
                this.unreadNumBytes = numBytes;
                this.message = new AtomicReference<>(message.retain());
                messageLock = new StampedLock().asWriteLock();
                this.timeout = timeout;
                this.inputStreams = inputStreams;
                this.exceptions = exceptions;
                this.future = new CompletableFuture<>();
            }

            ByteBuf blockInterruptiblyUntilCompleted() throws MongoInterruptedException {
                try {
                    return timeout.isInfinite()
                            ? future.get()
                            : future.get(timeout.remaining(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw exceptions.interrupted(e);
                } catch (ExecutionException e) {
                    throw exceptions.readFailed(assertNotNull(e.getCause()));
                } catch (TimeoutException e) {
                    throw exceptions.readTimedOut();
                }
            }

            boolean isCompleted() {
                return future.isDone();
            }

            void completeExceptionally(final Exception cause) {
                try {
                    future.completeExceptionally(cause);
                } finally {
                    detachAndReleaseMessage();
                }
            }

            /**
             * This method does not block while waiting for more data,
             * but may briefly block if called concurrently with itself, which is unlikely.
             * <p>
             * This method relies on streams in {@link #inputStreams} to contain all the data they may produce and report
             * {@link #END_OF_STREAM} when that data is exhausted instead of blocking. Common sense suggests that this must be the case,
             * but Java gRPC does not state such a guarantee. If this turns out to not be the case, and blocking harms us,
             * then we will have to do copying in {@link InputStreamUnmarshaller#parse(InputStream)}
             * from an {@link InputStream} to a buffer.</p>
             */
            void tryComplete() {
                try {
                    boolean timedOut;
                    if (timeout.isInfinite()) {
                        messageLock.lockInterruptibly();
                        timedOut = false;
                    } else {
                        timedOut = !messageLock.tryLock(timeout.remaining(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                    }
                    if (timedOut) {
                        completeExceptionally(exceptions.readTimedOut());
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    completeExceptionally(exceptions.interrupted(e));
                    return;
                }
                try {
                    uncheckedTryComplete();
                } finally {
                    messageLock.unlock();
                }
            }

            /**
             * Must be guarded with {@link #messageLock}.
             */
            private void uncheckedTryComplete() {
                // While we are trying to complete, we temporarily detach `message` to prevent other threads
                // from releasing it concurrently with us using it.
                // This `PendingRead` still may be completed exceptionally concurrently,
                // or it may have been completed normally.
                io.netty.buffer.ByteBuf detachedMessage = this.message.getAndSet(null);
                if (detachedMessage == null) {
                    assertTrue(isCompleted());
                    return;
                } else {
                    assertTrue(unreadNumBytes > 0);
                }
                try {
                    InputStream inputStream = inputStreams.peek();
                    while (unreadNumBytes > 0 && inputStream != null) {
                        int readNumBytes = detachedMessage.writeBytes(inputStream, unreadNumBytes);
                        if (readNumBytes == END_OF_STREAM) {
                            inputStreams.remove().close();
                            inputStream = inputStreams.peek();
                        } else {
                            unreadNumBytes -= readNumBytes;
                        }
                        if (unreadNumBytes > 0) {
                            if (timeout.expired()) {
                                throw exceptions.readTimedOut();
                            } else if (Thread.currentThread().isInterrupted()) {
                                throw exceptions.interrupted(null);
                            }
                        }
                    }
                } catch (Exception e) {
                    completeExceptionally(e);
                } finally {
                    if (unreadNumBytes == 0) {
                        complete(detachedMessage);
                    } else if (unreadNumBytes > 0) {
                        // reattach `detachedMessage` because we have not read everything we need
                        this.message.set(detachedMessage);
                        if (isCompleted()) {
                            assertTrue(future.isCompletedExceptionally());
                            detachAndReleaseMessage();
                        }
                    } else {
                        fail();
                    }
                }
            }

            private void complete(final io.netty.buffer.ByteBuf detachedMessage) {
                ByteBuf result = new NettyByteBuf(detachedMessage);
                result.flip();
                if (!future.complete(result)) {
                    assertTrue(future.isCompletedExceptionally());
                    result.release();
                }
            }

            private void detachAndReleaseMessage() {
                io.netty.buffer.ByteBuf detachedMessage = this.message.getAndSet(null);
                if (detachedMessage != null) {
                    detachedMessage.release();
                }
            }
        }
    }

    @ThreadSafe
    private static final class PendingWriteMarshaller implements Marshaller<PendingWrite> {
        PendingWriteMarshaller() {
        }

        @Override
        public InputStream stream(final PendingWrite value) {
            return value.detachedMessageInputStream();
        }

        @Override
        public PendingWrite parse(final InputStream stream) {
            throw fail();
        }
    }

    @ThreadSafe
    private static final class InputStreamUnmarshaller implements Marshaller<InputStream> {
        InputStreamUnmarshaller() {
        }

        @Override
        public InputStream stream(final InputStream value) {
            throw fail();
        }

        @Override
        @SuppressWarnings("try")
        public InputStream parse(
                // we must not close `stream`, Java gRPC does that
                final InputStream stream) {
            // We do not rely on `io.grpc.HasByteBuffer` because `HasByteBuffer.byteBufferSupported` is not always `true`,
            // and `HasByteBuffer.getByteBuffer` is really weird: on `io.grpc.internal.CompositeReadableBuffer` it returns only the
            // first buffer, and does not allow accessing the rest of them.
            //
            // We, however, rely on `Detachable` because it allows us to avoid copying bytes twice when reading.
            // We still have to copy them once to return `ByteBuf` from `GrpcStream.read`.
            //
            // We could call `ReadState.add` here, but Java gRPC expects us to rather do that in `ClientCall.Listener.onMessage`.
            // Let us hope that Java gRPC calls `onMessage` for each `parse` that completes normally, regardless of how bad things are,
            // otherwise, we have a resource leak. It would have been helpful if Java gRPC explicitly documented such a crucial guarantee,
            // because if it does not exist, then the Java gRPC API is inherently broken.
            assertTrue(stream instanceof Detachable);
            return ((Detachable) stream).detach();
        }
    }
}
