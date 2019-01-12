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

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.util.function.Consumer;

/**
 * Base class for builders of {@link TlsChannel}.
 */
public abstract class TlsChannelBuilder<T extends TlsChannelBuilder<T>> {

    final ByteChannel underlying;

    // @formatter:off
    Consumer<SSLSession> sessionInitCallback = new Consumer<SSLSession>() {
        @Override
        public void accept(final SSLSession session) {
        }
    };
    // @formatter:on
    boolean runTasks = true;
    BufferAllocator plainBufferAllocator = null;
    BufferAllocator encryptedBufferAllocator = null;
    boolean releaseBuffers = true;
    boolean waitForCloseConfirmation = false;

    TlsChannelBuilder(final ByteChannel underlying) {
        this.underlying = underlying;
    }

    abstract T getThis();

    /**
     * Whether CPU-intensive tasks are run or not. Default is to do run them. If
     * setting this <code>false</code>, the calling code should be prepared to handle
     * {@link NeedsTaskException}}
     */
    public T withRunTasks(final boolean runTasks) {
        this.runTasks = runTasks;
        return getThis();
    }

    /**
     * Set the {@link BufferAllocator} to use for unencrypted data.
     */
    public T withPlainBufferAllocator(final BufferAllocator bufferAllocator) {
        this.plainBufferAllocator = bufferAllocator;
        return getThis();
    }

    /**
     * Set the {@link BufferAllocator} to use for encrypted data.
     */
    public T withEncryptedBufferAllocator(final BufferAllocator bufferAllocator) {
        this.encryptedBufferAllocator = bufferAllocator;
        return getThis();
    }

    /**
     * Register a callback function to be executed when the TLS session is
     * established (or re-established). The supplied function will run in the
     * same thread as the rest of the handshake, so it should ideally run as
     * fast as possible.
     */
    public T withSessionInitCallback(final Consumer<SSLSession> sessionInitCallback) {
        this.sessionInitCallback = sessionInitCallback;
        return getThis();
    }

    /**
     * Whether to release unused buffers in the mid of connections. Equivalent to
     * OpenSSL's SSL_MODE_RELEASE_BUFFERS.
     * <p>
     * Default is to release. Releasing unused buffers is specially effective
     * in the case case of idle long-lived connections, when the memory footprint
     * can be reduced significantly. A potential reason for setting this value
     * to <code>false</code> is performance, since more releases means more
     * allocations, which have a cost. This is effectively a memory-time trade-off.
     * However, in most cases the default behavior makes sense.
     */
    public T withReleaseBuffers(final boolean releaseBuffers) {
        this.releaseBuffers = releaseBuffers;
        return getThis();
    }

    /**
     * <p> Whether to wait for TLS close confirmation when executing a local {@link TlsChannel#close()} on the channel.
     * If the underlying channel is blocking, setting this to <code>true</code> will block (potentially until it times
     * out, or indefinitely) the close operation until the counterpart confirms the close on their side (sending a
     * close_notify alert. If the underlying channel is non-blocking, setting this parameter to true is ineffective.
     * </p>
     *
     * <p> Setting this value to <code>true</code> emulates the behavior of {@link SSLSocket} when used in layered mode
     * (and without autoClose). </p>
     *
     * <p> Even when this behavior is enabled, the close operation will not propagate any {@link IOException} thrown
     * during the TLS close exchange and just proceed to close the underlying channel. </p>
     *
     * <p> Default is to not wait and close immediately. The proper closing procedure can be initiated at any moment
     * using {@link TlsChannel#shutdown()}.</p>
     *
     * @see TlsChannel#shutdown()
     */
    public T withWaitForCloseConfirmation(final boolean waitForCloseConfirmation) {
        this.waitForCloseConfirmation = waitForCloseConfirmation;
        return getThis();
    }

}
