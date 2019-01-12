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
 *
 * Original Work: MIT License, Copyright (c) [2015-2018] all contributors
 * https://github.com/marianobarrios/tls-channel
 */

package com.mongodb.internal.connection.tlschannel.impl;

import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.connection.tlschannel.NeedsReadException;
import com.mongodb.internal.connection.tlschannel.NeedsTaskException;
import com.mongodb.internal.connection.tlschannel.NeedsWriteException;
import com.mongodb.internal.connection.tlschannel.TrackingAllocator;
import com.mongodb.internal.connection.tlschannel.WouldBlockException;
import com.mongodb.internal.connection.tlschannel.util.TlsChannelCallbackException;
import com.mongodb.internal.connection.tlschannel.util.Util;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.String.format;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;

public class TlsChannelImpl implements ByteChannel {

    private static final Logger LOGGER = Loggers.getLogger("connection.tls");

    private static final int BUFFERS_INITIAL_SIZE = 4096;

    /**
     * Official TLS max data size is 2^14 = 16k. Use 1024 more to account for
     * the overhead
     */
    static final int MAX_TLS_PACKET_SIZE = 17 * 1024;

    private static class UnwrapResult {
        final int bytesProduced;
        final HandshakeStatus lastHandshakeStatus;
        final boolean wasClosed;

        UnwrapResult(final int bytesProduced, final HandshakeStatus lastHandshakeStatus, final boolean wasClosed) {
            this.bytesProduced = bytesProduced;
            this.lastHandshakeStatus = lastHandshakeStatus;
            this.wasClosed = wasClosed;
        }
    }

    private static class WrapResult {
        final int bytesConsumed;
        final HandshakeStatus lastHandshakeStatus;

        WrapResult(final int bytesConsumed, final HandshakeStatus lastHandshakeStatus) {
            this.bytesConsumed = bytesConsumed;
            this.lastHandshakeStatus = lastHandshakeStatus;
        }
    }

    /**
     * Used to signal EOF conditions from the underlying channel
     */
    public static class EofException extends Exception {

        private static final long serialVersionUID = -9215047770779892445L;

        /**
         * For efficiency, override this method to do nothing.
         */
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }

    }

    private final ReadableByteChannel readChannel;
    private final WritableByteChannel writeChannel;
    private final SSLEngine engine;
    private BufferHolder inEncrypted;
    private final Consumer<SSLSession> initSessionCallback;

    private final boolean runTasks;
    private final TrackingAllocator encryptedBufAllocator;
    private final TrackingAllocator plainBufAllocator;
    private final boolean waitForCloseConfirmation;

    // @formatter:off
    public TlsChannelImpl(
            final ReadableByteChannel readChannel,
            final WritableByteChannel writeChannel,
            final SSLEngine engine,
            final Optional<BufferHolder> inEncrypted,
            final Consumer<SSLSession> initSessionCallback,
            final boolean runTasks,
            final TrackingAllocator plainBufAllocator,
            final TrackingAllocator encryptedBufAllocator,
            final boolean releaseBuffers,
            final boolean waitForCloseConfirmation) {
        // @formatter:on
        this.readChannel = readChannel;
        this.writeChannel = writeChannel;
        this.engine = engine;
        this.inEncrypted = inEncrypted.orElseGet(new Supplier<BufferHolder>() {
            @Override
            public BufferHolder get() {
                return new BufferHolder(
                        "inEncrypted",
                        encryptedBufAllocator,
                        BUFFERS_INITIAL_SIZE,
                        MAX_TLS_PACKET_SIZE,
                        false /* plainData */,
                        releaseBuffers);
            }
        });
        this.initSessionCallback = initSessionCallback;
        this.runTasks = runTasks;
        this.plainBufAllocator = plainBufAllocator;
        this.encryptedBufAllocator = encryptedBufAllocator;
        this.waitForCloseConfirmation = waitForCloseConfirmation;
        inPlain = new BufferHolder(
                "inPlain",
                plainBufAllocator,
                BUFFERS_INITIAL_SIZE,
                MAX_TLS_PACKET_SIZE,
                true /* plainData */,
                releaseBuffers);
        outEncrypted = new BufferHolder(
                "outEncrypted",
                encryptedBufAllocator,
                BUFFERS_INITIAL_SIZE,
                MAX_TLS_PACKET_SIZE,
                false /* plainData */,
                releaseBuffers);
    }

    private final Lock initLock = new ReentrantLock();
    private final Lock readLock = new ReentrantLock();
    private final Lock writeLock = new ReentrantLock();

    private volatile boolean negotiated = false;

    /**
     * Whether a IOException was received from the underlying channel or from
     * the {@link SSLEngine}.
     */
    private volatile boolean invalid = false;

    /**
     * Whether a close_notify was already sent.
     */
    private volatile boolean shutdownSent = false;

    /**
     * Whether a close_notify was already received.
     */
    private volatile boolean shutdownReceived = false;

    // decrypted data from inEncrypted
    private BufferHolder inPlain;

    // contains data encrypted to send to the underlying channel
    private BufferHolder outEncrypted;

    // handshake wrap() method calls need a buffer to read from, even when they
    // actually do not read anything
    private final ByteBufferSet dummyOut = new ByteBufferSet(new ByteBuffer[]{});

    public Consumer<SSLSession> getSessionInitCallback() {
        return initSessionCallback;
    }

    public TrackingAllocator getPlainBufferAllocator() {
        return plainBufAllocator;
    }

    public TrackingAllocator getEncryptedBufferAllocator() {
        return encryptedBufAllocator;
    }

    // read

    public long read(final ByteBufferSet dest) throws IOException {
        checkReadBuffer(dest);
        if (!dest.hasRemaining()) {
            return 0;
        }
        handshake();
        readLock.lock();
        try {
            if (invalid || shutdownSent) {
                throw new ClosedChannelException();
            }
            HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
            int bytesToReturn = inPlain.nullOrEmpty() ? 0 : inPlain.buffer.position();
            while (true) {
                if (bytesToReturn > 0) {
                    if (inPlain.nullOrEmpty()) {
                        return bytesToReturn;
                    } else {
                        return transferPendingPlain(dest);
                    }
                }
                if (shutdownReceived) {
                    return -1;
                }
                Util.assertTrue(inPlain.nullOrEmpty());
                switch (handshakeStatus) {
                    case NEED_UNWRAP:
                    case NEED_WRAP:
                        bytesToReturn = handshake(Optional.of(dest), Optional.of(handshakeStatus));
                        handshakeStatus = NOT_HANDSHAKING;
                        break;
                    case NOT_HANDSHAKING:
                    case FINISHED:
                        UnwrapResult res = readAndUnwrap(Optional.of(dest), NOT_HANDSHAKING /* statusCondition */,
                                false /* closing */);
                        if (res.wasClosed) {
                            return -1;
                        }
                        bytesToReturn = res.bytesProduced;
                        handshakeStatus = res.lastHandshakeStatus;
                        break;
                    case NEED_TASK:
                        handleTask();
                        handshakeStatus = engine.getHandshakeStatus();
                        break;
                    default:
                        throw new SSLHandshakeException("Unsupported handshake status: " + handshakeStatus);
                }
            }
        } catch (EofException e) {
            return -1;
        } finally {
            readLock.unlock();
        }
    }

    private void handleTask() throws NeedsTaskException {
        if (runTasks) {
            engine.getDelegatedTask().run();
        } else {
            throw new NeedsTaskException(engine.getDelegatedTask());
        }
    }

    private int transferPendingPlain(final ByteBufferSet dstBuffers) {
        ((Buffer) inPlain.buffer).flip(); // will read
        int bytes = dstBuffers.putRemaining(inPlain.buffer);
        inPlain.buffer.compact(); // will write
        boolean disposed = inPlain.release();
        if (!disposed) {
            inPlain.zeroRemaining();
        }
        return bytes;
    }

    private UnwrapResult unwrapLoop(final Optional<ByteBufferSet> dest, final HandshakeStatus statusCondition, final boolean closing)
            throws SSLException {
        ByteBufferSet effDest = dest.orElseGet(new Supplier<ByteBufferSet>() {
            @Override
            public ByteBufferSet get() {
                inPlain.prepare();
                return new ByteBufferSet(inPlain.buffer);
            }
        });
        while (true) {
            Util.assertTrue(inPlain.nullOrEmpty());
            SSLEngineResult result = callEngineUnwrap(effDest);
            /*
             * Note that data can be returned even in case of overflow, in that
             * case, just return the data.
             */
            if (result.bytesProduced() > 0 || result.getStatus() == Status.BUFFER_UNDERFLOW
                    || !closing && result.getStatus() == Status.CLOSED
                    || result.getHandshakeStatus() != statusCondition) {
                boolean wasClosed = result.getStatus() == Status.CLOSED;
                return new UnwrapResult(result.bytesProduced(), result.getHandshakeStatus(), wasClosed);
            }
            if (result.getStatus() == Status.BUFFER_OVERFLOW) {
                if (dest.isPresent() && effDest == dest.get()) {
                    /*
                     * The client-supplier buffer is not big enough. Use the
                     * internal inPlain buffer, also ensure that it is bigger
                     * than the too-small supplied one.
                     */
                    inPlain.prepare();
                    ensureInPlainCapacity(Math.min(((int) dest.get().remaining()) * 2, MAX_TLS_PACKET_SIZE));
                } else {
                    inPlain.enlarge();
                }
                // inPlain changed, re-create the wrapper
                effDest = new ByteBufferSet(inPlain.buffer);
            }
        }
    }

    private SSLEngineResult callEngineUnwrap(final ByteBufferSet dest) throws SSLException {
        ((Buffer) inEncrypted.buffer).flip();
        try {
            SSLEngineResult result = engine.unwrap(inEncrypted.buffer, dest.array, dest.offset, dest.length);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("engine.unwrap() result [%s]. Engine status: %s; inEncrypted %s; inPlain: %s",
                        Util.resultToString(result), result.getHandshakeStatus(), inEncrypted, dest));
            }
            return result;
        } catch (SSLException e) {
            // something bad was received from the underlying channel, we cannot
            // continue
            invalid = true;
            throw e;
        } finally {
            inEncrypted.buffer.compact();
        }
    }

    private int readFromChannel() throws IOException, EofException {
        try {
            return readFromChannel(readChannel, inEncrypted.buffer);
        } catch (WouldBlockException e) {
            throw e;
        } catch (IOException e) {
            invalid = true;
            throw e;
        }
    }

    public static int readFromChannel(final ReadableByteChannel readChannel, final ByteBuffer buffer)
            throws IOException, EofException {
        Util.assertTrue(buffer.hasRemaining());
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Reading from channel");
        }
        int c = readChannel.read(buffer); // IO block
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Read from channel; response: %s, buffer: %s", c, buffer));
        }
        if (c == -1) {
            throw new EofException();
        }
        if (c == 0) {
            throw new NeedsReadException();
        }
        return c;
    }

    // write

    public long write(final ByteBufferSet source) throws IOException {
        /*
         * Note that we should enter the write loop even in the case that the source buffer has no remaining bytes,
         * as it could be the case, in non-blocking usage, that the user is forced to call write again after the
         * underlying channel is available for writing, just to write pending encrypted bytes.
         */
        handshake();
        writeLock.lock();
        try {
            if (invalid || shutdownSent) {
                throw new ClosedChannelException();
            }
            return wrapAndWrite(source);
        } finally {
            writeLock.unlock();
        }
    }

    private long wrapAndWrite(final ByteBufferSet source) throws IOException {
        long bytesToConsume = source.remaining();
        long bytesConsumed = 0;
        outEncrypted.prepare();
        try {
            while (true) {
                writeToChannel();
                if (bytesConsumed == bytesToConsume) {
                    return bytesToConsume;
                }
                WrapResult res = wrapLoop(source);
                bytesConsumed += res.bytesConsumed;
            }
        } finally {
            outEncrypted.release();
        }
    }

    private WrapResult wrapLoop(final ByteBufferSet source) throws SSLException {
        while (true) {
            SSLEngineResult result = callEngineWrap(source);
            switch (result.getStatus()) {
                case OK:
                case CLOSED:
                    return new WrapResult(result.bytesConsumed(), result.getHandshakeStatus());
                case BUFFER_OVERFLOW:
                    Util.assertTrue(result.bytesConsumed() == 0);
                    outEncrypted.enlarge();
                    break;
                case BUFFER_UNDERFLOW:
                    throw new IllegalStateException();
                default:
                    throw new AssertionError("Unexpected status: " + result.getStatus());
            }
        }
    }

    private SSLEngineResult callEngineWrap(final ByteBufferSet source) throws SSLException {
        try {
            SSLEngineResult result = engine.wrap(source.array, source.offset, source.length, outEncrypted.buffer);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("engine.wrap() result: [%s]; engine status: %s; srcBuffer: %s, outEncrypted: %s",
                        Util.resultToString(result), result.getHandshakeStatus(), source, outEncrypted));
            }
            return result;
        } catch (SSLException e) {
            invalid = true;
            throw e;
        }
    }

    private void ensureInPlainCapacity(final int newCapacity) {
        if (inPlain.buffer.capacity() < newCapacity) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("inPlain buffer too small, increasing from %s to %s", inPlain.buffer.capacity(), newCapacity));
            }
            inPlain.resize(newCapacity);
        }
    }

    private void writeToChannel() throws IOException {
        if (outEncrypted.buffer.position() == 0) {
            return;
        }
        ((Buffer) outEncrypted.buffer).flip();
        try {
            try {
                writeToChannel(writeChannel, outEncrypted.buffer);
            } catch (WouldBlockException e) {
                throw e;
            } catch (IOException e) {
                invalid = true;
                throw e;
            }
        } finally {
            outEncrypted.buffer.compact();
        }
    }

    private static void writeToChannel(final WritableByteChannel channel, final ByteBuffer src) throws IOException {
        while (src.hasRemaining()) {
            LOGGER.trace(format("Writing to channel: %s", src));
            int c = channel.write(src);
            if (c == 0) {
                /*
                 * If no bytesProduced were written, it means that the socket is
                 * non-blocking and needs more buffer space, so stop the loop
                 */
                throw new NeedsWriteException();
            }
            // blocking SocketChannels can write less than all the bytesProduced
            // just before an error the loop forces the exception
        }
    }

    // handshake and close

    /**
     * Force new negotiation
     */
    public void renegotiate() throws IOException {
        try {
            doHandshake(true /* force */);
        } catch (EofException e) {
            throw new ClosedChannelException();
        }
    }

    /**
     * Do a negotiation if this connection is new and it hasn't been done
     * already.
     */
    public void handshake() throws IOException {
        try {
            doHandshake(false /* force */);
        } catch (EofException e) {
            throw new ClosedChannelException();
        }
    }

    private void doHandshake(final boolean force) throws IOException, EofException {
        if (!force && negotiated) {
            return;
        }
        if (invalid || shutdownSent) {
            throw new ClosedChannelException();
        }
        initLock.lock();
        try {
            if (force || !negotiated) {
                engine.beginHandshake();
                LOGGER.trace("Called engine.beginHandshake()");
                handshake(Optional.<ByteBufferSet>empty(), Optional.<HandshakeStatus>empty());
                // call client code
                try {
                    initSessionCallback.accept(engine.getSession());
                } catch (Exception e) {
                    LOGGER.trace("client code threw exception in session initialization callback", e);
                    throw new TlsChannelCallbackException("session initialization callback failed", e);
                }
                negotiated = true;
            }
        } finally {
            initLock.unlock();
        }
    }

    private int handshake(final Optional<ByteBufferSet> dest, final Optional<HandshakeStatus> handshakeStatus)
            throws IOException, EofException {
        readLock.lock();
        try {
            writeLock.lock();
            try {
                Util.assertTrue(inPlain.nullOrEmpty());
                outEncrypted.prepare();
                try {
                    writeToChannel(); // IO block
                    return handshakeLoop(dest, handshakeStatus);
                } finally {
                    outEncrypted.release();
                }
            } finally {
                writeLock.unlock();
            }
        } finally {
            readLock.unlock();
        }
    }

    private int handshakeLoop(final Optional<ByteBufferSet> dest, final Optional<HandshakeStatus> handshakeStatus)
            throws IOException, EofException {
        Util.assertTrue(inPlain.nullOrEmpty());
        HandshakeStatus status = handshakeStatus.orElseGet(new Supplier<HandshakeStatus>() {
            @Override
            public HandshakeStatus get() {
                return engine.getHandshakeStatus();
            }
        });
        while (true) {
            switch (status) {
                case NEED_WRAP:
                    Util.assertTrue(outEncrypted.nullOrEmpty());
                    WrapResult wrapResult = wrapLoop(dummyOut);
                    status = wrapResult.lastHandshakeStatus;
                    writeToChannel(); // IO block
                    break;
                case NEED_UNWRAP:
                    UnwrapResult res = readAndUnwrap(dest, NEED_UNWRAP /* statusCondition */, false /* closing */);
                    status = res.lastHandshakeStatus;
                    if (res.bytesProduced > 0) {
                        return res.bytesProduced;
                    }
                    break;
                case NOT_HANDSHAKING:
                    /*
                     * This should not really happen using SSLEngine, because
                     * handshaking ends with a FINISHED status. However, we accept
                     * this value to permit the use of a pass-through stub engine
                     * with no encryption.
                     */
                    return 0;
                case NEED_TASK:
                    handleTask();
                    status = engine.getHandshakeStatus();
                    break;
                case FINISHED:
                    return 0;
                default:
                    throw new AssertionError("Unexpected status: " + status);
            }
        }
    }

    private UnwrapResult readAndUnwrap(final Optional<ByteBufferSet> dest, final HandshakeStatus statusCondition, final boolean closing)
            throws IOException, EofException {
        inEncrypted.prepare();
        try {
            while (true) {
                Util.assertTrue(inPlain.nullOrEmpty());
                UnwrapResult res = unwrapLoop(dest, statusCondition, closing);
                if (res.bytesProduced > 0 || res.lastHandshakeStatus != statusCondition || !closing && res.wasClosed) {
                    if (res.wasClosed) {
                        shutdownReceived = true;
                    }
                    return res;
                }
                if (!inEncrypted.buffer.hasRemaining()) {
                    inEncrypted.enlarge();
                }
                readFromChannel(); // IO block
            }
        } finally {
            inEncrypted.release();
        }
    }

    public void close() throws IOException {
        tryShutdown();
        writeChannel.close();
        readChannel.close();
        /*
         * After closing the underlying channels, locks should be taken fast.
         */
        readLock.lock();
        try {
            writeLock.lock();
            try {
                freeBuffers();
            } finally {
                writeLock.unlock();
            }
        } finally {
            readLock.unlock();
        }
    }

    private void tryShutdown() {
        if (!readLock.tryLock()) {
            return;
        }
        try {
            if (!writeLock.tryLock()) {
                return;
            }
            try {
                if (!shutdownSent) {
                    try {
                        boolean closed = shutdown();
                        if (!closed && waitForCloseConfirmation) {
                            shutdown();
                        }
                    } catch (Throwable e) {
                        LOGGER.debug(format("error doing TLS shutdown on close(), continuing: %s", e.getMessage()));
                    }
                }
            } finally {
                writeLock.unlock();
            }
        } finally {
            readLock.unlock();
        }
    }

    public boolean shutdown() throws IOException {
        readLock.lock();
        try {
            writeLock.lock();
            try {
                if (invalid) {
                    throw new ClosedChannelException();
                }
                if (!shutdownSent) {
                    shutdownSent = true;
                    outEncrypted.prepare();
                    try {
                        writeToChannel(); // IO block
                        engine.closeOutbound();
                        wrapLoop(dummyOut);
                        writeToChannel(); // IO block
                    } finally {
                        outEncrypted.release();
                    }
                    /*
                     * If this side is the first to send close_notify, then,
                     * inbound is not done and false should be returned (so the
                     * client waits for the response. If this side is the
                     * second, then inbound was already done, and we can return
                     * true.
                     */
                    if (shutdownReceived) {
                        freeBuffers();
                    }
                    return shutdownReceived;
                }
                /*
                 * If we reach this point, then we just have to read the close
                 * notification from the client. Only try to do it if necessary,
                 * to make this method idempotent.
                 */
                if (!shutdownReceived) {
                    try {
                        // IO block
                        readAndUnwrap(Optional.<ByteBufferSet>empty(), NEED_UNWRAP /* statusCondition */, true /* closing */);
                        Util.assertTrue(shutdownReceived);
                    } catch (EofException e) {
                        throw new ClosedChannelException();
                    }
                }
                freeBuffers();
                return true;
            } finally {
                writeLock.unlock();
            }
        } finally {
            readLock.unlock();
        }
    }

    private void freeBuffers() {
        if (inEncrypted != null) {
            inEncrypted.dispose();
            inEncrypted = null;
        }
        if (inPlain != null) {
            inPlain.dispose();
            inPlain = null;
        }
        if (outEncrypted != null) {
            outEncrypted.dispose();
            outEncrypted = null;
        }
    }

    public boolean isOpen() {
        return !invalid && writeChannel.isOpen() && readChannel.isOpen();
    }

    public static void checkReadBuffer(final ByteBufferSet dest) {
        if (dest.isReadOnly()) {
            throw new IllegalArgumentException();
        }
    }

    public SSLEngine engine() {
        return engine;
    }

    public boolean getRunTasks() {
        return runTasks;
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
        return (int) read(new ByteBufferSet(dst));
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        return (int) write(new ByteBufferSet(src));
    }

    public boolean shutdownReceived() {
        return shutdownReceived;
    }

    public boolean shutdownSent() {
        return shutdownSent;
    }

    public ReadableByteChannel plainReadableChannel() {
        return readChannel;
    }

    public WritableByteChannel plainWritableChannel() {
        return writeChannel;
    }
}
