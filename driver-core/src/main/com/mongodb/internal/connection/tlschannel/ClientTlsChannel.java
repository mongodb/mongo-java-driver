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

package com.mongodb.internal.connection.tlschannel;

import com.mongodb.internal.connection.tlschannel.impl.BufferHolder;
import com.mongodb.internal.connection.tlschannel.impl.ByteBufferSet;
import com.mongodb.internal.connection.tlschannel.impl.TlsChannelImpl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channel;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A client-side {@link TlsChannel}.
 */
public final class ClientTlsChannel implements TlsChannel {

    /**
     * Builder of {@link ClientTlsChannel}
     */
    public static final class Builder extends TlsChannelBuilder<Builder> {

        private Supplier<SSLEngine> sslEngineFactory;

        private Builder(final ByteChannel underlying, final SSLEngine sslEngine) {
            super(underlying);
            this.sslEngineFactory = new Supplier<SSLEngine>() {
                @Override
                public SSLEngine get() {
                    return sslEngine;
                }
            };
        }

        private Builder(final ByteChannel underlying, final SSLContext sslContext) {
            super(underlying);
            this.sslEngineFactory = new Supplier<SSLEngine>() {
                @Override
                public SSLEngine get() {
                    return defaultSSLEngineFactory(sslContext);
                }
            };
        }

        @Override
        Builder getThis() {
            return this;
        }

        public ClientTlsChannel build() {
            return new ClientTlsChannel(underlying, sslEngineFactory.get(), sessionInitCallback, runTasks,
                    plainBufferAllocator, encryptedBufferAllocator, releaseBuffers, waitForCloseConfirmation);
        }

    }

    private static SSLEngine defaultSSLEngineFactory(final SSLContext sslContext) {
        SSLEngine engine = sslContext.createSSLEngine();
        engine.setUseClientMode(true);
        return engine;
    }

    /**
     * Create a new {@link Builder}, configured with a underlying
     * {@link Channel} and a fixed {@link SSLEngine}.
     *
     * @param underlying a reference to the underlying {@link ByteChannel}
     * @param sslEngine  the engine to use with this channel
     */
    public static Builder newBuilder(final ByteChannel underlying, final SSLEngine sslEngine) {
        return new Builder(underlying, sslEngine);
    }

    /**
     * Create a new {@link Builder}, configured with a underlying
     * {@link Channel} and a {@link SSLContext}.
     *
     * @param underlying a reference to the underlying {@link ByteChannel}
     * @param sslContext a context to use with this channel, it will be used to create a client {@link SSLEngine}.
     */
    public static Builder newBuilder(final ByteChannel underlying, final SSLContext sslContext) {
        return new Builder(underlying, sslContext);
    }

    private final ByteChannel underlying;
    private final TlsChannelImpl impl;

    private ClientTlsChannel(
            final ByteChannel underlying,
            final SSLEngine engine,
            final Consumer<SSLSession> sessionInitCallback,
            final boolean runTasks,
            final BufferAllocator plainBufAllocator,
            final BufferAllocator encryptedBufAllocator,
            final boolean releaseBuffers,
            final boolean waitForCloseNotifyOnClose) {
        if (!engine.getUseClientMode()) {
            throw new IllegalArgumentException("SSLEngine must be in client mode");
        }
        this.underlying = underlying;
        TrackingAllocator trackingPlainBufAllocator = new TrackingAllocator(plainBufAllocator);
        TrackingAllocator trackingEncryptedAllocator = new TrackingAllocator(encryptedBufAllocator);
        impl = new TlsChannelImpl(underlying, underlying, engine, Optional.<BufferHolder>empty(), sessionInitCallback, runTasks,
                trackingPlainBufAllocator, trackingEncryptedAllocator, releaseBuffers, waitForCloseNotifyOnClose);
    }

    @Override
    public ByteChannel getUnderlying() {
        return underlying;
    }

    @Override
    public SSLEngine getSslEngine() {
        return impl.engine();
    }

    @Override
    public Consumer<SSLSession> getSessionInitCallback() {
        return impl.getSessionInitCallback();
    }

    @Override
    public TrackingAllocator getPlainBufferAllocator() {
        return impl.getPlainBufferAllocator();
    }

    @Override
    public TrackingAllocator getEncryptedBufferAllocator() {
        return impl.getEncryptedBufferAllocator();
    }

    @Override
    public boolean getRunTasks() {
        return impl.getRunTasks();
    }

    @Override
    public long read(final ByteBuffer[] dstBuffers, final int offset, final int length) throws IOException {
        ByteBufferSet dest = new ByteBufferSet(dstBuffers, offset, length);
        TlsChannelImpl.checkReadBuffer(dest);
        return impl.read(dest);
    }

    @Override
    public long read(final ByteBuffer[] dstBuffers) throws IOException {
        return read(dstBuffers, 0, dstBuffers.length);
    }

    @Override
    public int read(final ByteBuffer dstBuffer) throws IOException {
        return (int) read(new ByteBuffer[]{dstBuffer});
    }

    @Override
    public long write(final ByteBuffer[] srcBuffers, final int offset, final int length) throws IOException {
        ByteBufferSet source = new ByteBufferSet(srcBuffers, offset, length);
        return impl.write(source);
    }

    @Override
    public long write(final ByteBuffer[] outs) throws IOException {
        return write(outs, 0, outs.length);
    }

    @Override
    public int write(final ByteBuffer srcBuffer) throws IOException {
        return (int) write(new ByteBuffer[]{srcBuffer});
    }

    @Override
    public void renegotiate() throws IOException {
        impl.renegotiate();
    }

    @Override
    public void handshake() throws IOException {
        impl.handshake();
    }

    @Override
    public void close() throws IOException {
        impl.close();
    }

    @Override
    public boolean isOpen() {
        return impl.isOpen();
    }

    @Override
    public boolean shutdown() throws IOException {
        return impl.shutdown();
    }

    @Override
    public boolean shutdownReceived() {
        return impl.shutdownReceived();
    }

    @Override
    public boolean shutdownSent() {
        return impl.shutdownSent();
    }

}
