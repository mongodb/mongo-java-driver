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
 * Original Work: MIT License, Copyright (c) [2015-2020] all contributors
 * https://github.com/marianobarrios/tls-channel
 */

package com.mongodb.internal.connection.tlschannel;

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.util.function.Consumer;

/** Base class for builders of {@link TlsChannel}. */
public abstract class TlsChannelBuilder<T extends TlsChannelBuilder<T>> {

  final ByteChannel underlying;

  // @formatter:off
  Consumer<SSLSession> sessionInitCallback = session -> {};
  // @formatter:on
  boolean runTasks = true;
  BufferAllocator plainBufferAllocator = TlsChannel.defaultPlainBufferAllocator;
  BufferAllocator encryptedBufferAllocator = TlsChannel.defaultEncryptedBufferAllocator;
  boolean releaseBuffers = true;
  boolean waitForCloseConfirmation = false;

  TlsChannelBuilder(ByteChannel underlying) {
    this.underlying = underlying;
  }

  abstract T getThis();

  /**
   * Whether CPU-intensive tasks are run or not. Default is to do run them. If setting this <code>
   * false</code>, the calling code should be prepared to handle {@link NeedsTaskException}
   *
   * @param runTasks whether to run tasks
   * @return this object
   */
  public T withRunTasks(boolean runTasks) {
    this.runTasks = runTasks;
    return getThis();
  }

  /**
   * Set the {@link BufferAllocator} to use for unencrypted data. By default a {@link
   * HeapBufferAllocator} is used, as this buffers are used to supplement user-supplied ones when
   * dealing with too big a TLS record, that is, they operate entirely inside the JVM.
   *
   * @param bufferAllocator the buffer allocator
   * @return this object
   */
  public T withPlainBufferAllocator(BufferAllocator bufferAllocator) {
    this.plainBufferAllocator = bufferAllocator;
    return getThis();
  }

  /**
   * Set the {@link BufferAllocator} to use for encrypted data. By default a {@link
   * DirectBufferAllocator} is used, as this data is usually read from or written to native sockets.
   *
   * @param bufferAllocator the buffer allocator
   * @return this object
   */
  public T withEncryptedBufferAllocator(BufferAllocator bufferAllocator) {
    this.encryptedBufferAllocator = bufferAllocator;
    return getThis();
  }

  /**
   * Register a callback function to be executed when the TLS session is established (or
   * re-established). The supplied function will run in the same thread as the rest of the
   * handshake, so it should ideally run as fast as possible.
   *
   * @param sessionInitCallback the session initialization callback
   * @return this object
   */
  public T withSessionInitCallback(Consumer<SSLSession> sessionInitCallback) {
    this.sessionInitCallback = sessionInitCallback;
    return getThis();
  }

  /**
   * Whether to release unused buffers in the mid of connections. Equivalent to OpenSSL's
   * SSL_MODE_RELEASE_BUFFERS.
   *
   * <p>Default is to release. Releasing unused buffers is specially effective in the case case of
   * idle long-lived connections, when the memory footprint can be reduced significantly. A
   * potential reason for setting this value to <code>false</code> is performance, since more
   * releases means more allocations, which have a cost. This is effectively a memory-time
   * trade-off. However, in most cases the default behavior makes sense.
   *
   * @param releaseBuffers whether to release buffers
   * @return this object
   */
  public T withReleaseBuffers(boolean releaseBuffers) {
    this.releaseBuffers = releaseBuffers;
    return getThis();
  }

  /**
   * Whether to wait for TLS close confirmation when executing a local {@link TlsChannel#close()} on
   * the channel. If the underlying channel is blocking, setting this to <code>true</code> will
   * block (potentially until it times out, or indefinitely) the close operation until the
   * counterpart confirms the close on their side (sending a close_notify alert. If the underlying
   * channel is non-blocking, setting this parameter to true is ineffective.
   *
   * <p>Setting this value to <code>true</code> emulates the behavior of {@link SSLSocket} when used
   * in layered mode (and without autoClose).
   *
   * <p>Even when this behavior is enabled, the close operation will not propagate any {@link
   * IOException} thrown during the TLS close exchange and just proceed to close the underlying
   * channel.
   *
   * <p>Default is to not wait and close immediately. The proper closing procedure can be initiated
   * at any moment using {@link TlsChannel#shutdown()}.
   *
   * @param waitForCloseConfirmation whether to wait for close confirmation
   * @return this object
   * @see TlsChannel#shutdown()
   */
  public T withWaitForCloseConfirmation(boolean waitForCloseConfirmation) {
    this.waitForCloseConfirmation = waitForCloseConfirmation;
    return getThis();
  }
}
