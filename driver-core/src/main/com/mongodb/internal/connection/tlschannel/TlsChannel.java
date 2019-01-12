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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

/**
 * A ByteChannel interface to a TLS (Transport Layer Security) connection.
 *
 * <p>
 * Instances that implement this interface delegate all cryptographic operations
 * to the standard Java TLS implementation: SSLEngine; effectively hiding it
 * behind an easy-to-use streaming API, that allows to securitize JVM
 * applications with minimal added complexity.
 *
 * <p>
 * In other words, an interface that allows the programmer to have TLS using the
 * same standard socket API used for plaintext, just like OpenSSL does for C,
 * only for Java.
 *
 * <p>
 * Note that this is an API adapter, not a cryptographic implementation: with
 * the exception of a few bytesProduced of parsing at the beginning of the connection,
 * to look for the SNI, the whole protocol implementation is done by the
 * SSLEngine. Both the SSLContext and SSLEngine are supplied by the client;
 * these classes are the ones responsible for protocol configuration, including
 * hostname validation, client-side authentication, etc.
 *
 * <p>
 * A TLS channel is created by using one of its subclasses. They will
 * take an existing {@link ByteChannel} (typically, but not necessarily, a
 * {@link SocketChannel}) and a {@link SSLEngine}.
 *
 * <p>
 * It should be noted that this interface extends {@link ByteChannel} as a
 * design compromise, but it does not follow its interface completely. In
 * particular, in case of underlying non-blocking channels, when it is not
 * possible to complete an operation, no zero is returned, but an
 * {@link WouldBlockException}. This divergence from the base interface is
 * needed because both a <code>read</code> and a <code>write</code> operation
 * can run out of both bytesProduced for reading and buffer space for writing, as a
 * handshake (a bidirectional operation) can happen at any moment. The user
 * would use a {@link Selector} to wait for the expected condition
 * <em>of the underlying channel</em>, and should know which operation to
 * register.
 *
 * <p>
 * On top of that, operations can also fail to complete due to asynchronous
 * tasks; this is communicated using a {@link NeedsTaskException}. This behavior
 * is controlled by the {@link #getRunTasks()} attribute. This allows the user
 * to execute CPU-intensive tasks out of the selector loop.
 */
public interface TlsChannel extends ByteChannel, GatheringByteChannel, ScatteringByteChannel {

    /**
     * Return a reference to the underlying {@link ByteChannel}.
     */
    ByteChannel getUnderlying();

    /**
     * Return a reference to the {@link SSLEngine} used.
     *
     * @return the engine reference if present, or <code>null</code> if unknown
     * (that can happen in server-side channels before the SNI is
     * parsed).
     */
    SSLEngine getSslEngine();

    /**
     * Return the callback function to be executed when the TLS session is
     * established (or re-established).
     *
     * @see TlsChannelBuilder#withSessionInitCallback(Consumer)
     */
    Consumer<SSLSession> getSessionInitCallback();

    /**
     * Return the {@link BufferAllocator} to use for unencrypted data. Actually, a decorating subclass is returned,
     * which contains allocation statistics for this channel.
     *
     * @see TlsChannelBuilder#withPlainBufferAllocator(BufferAllocator)
     * @see TrackingAllocator
     */
    TrackingAllocator getPlainBufferAllocator();

    /**
     * Return the {@link BufferAllocator} to use for encrypted data. Actually, a decorating subclass is returned,
     * which contains allocation statistics for this channel.
     *
     * @see TlsChannelBuilder#withEncryptedBufferAllocator(BufferAllocator)
     * @see TrackingAllocator
     */
    TrackingAllocator getEncryptedBufferAllocator();

    /**
     * Return whether CPU-intensive tasks are run or not.
     *
     * @see TlsChannelBuilder#withRunTasks(boolean)
     */
    boolean getRunTasks();

    /**
     * Reads a sequence of bytesProduced from this channel into the given buffer.
     *
     * <p>
     * An attempt is made to read up to <i>r</i> bytesProduced from the channel, where
     * <i>r</i> is the number of bytesProduced remaining in the buffer, that is,
     * <tt>dst.remaining()</tt>, at the moment this method is invoked.
     *
     * <p>
     * Suppose that a byte sequence of length <i>n</i> is read, where <tt>0</tt>
     * &nbsp;<tt>&lt;=</tt>&nbsp;<i>n</i>&nbsp;<tt>&lt;=</tt>&nbsp;<i>r</i>.
     * This byte sequence will be transferred into the buffer so that the first
     * byte in the sequence is at index <i>p</i> and the last byte is at index
     * <i>p</i>&nbsp;<tt>+</tt>&nbsp;<i>n</i>&nbsp;<tt>-</tt>&nbsp;<tt>1</tt>,
     * where <i>p</i> is the buffer's position at the moment this method is
     * invoked. Upon return the buffer's position will be equal to <i>p</i>
     * &nbsp;<tt>+</tt>&nbsp;<i>n</i>; its limit will not have changed.
     *
     * <p>
     * A read operation might not fill the buffer, and in fact it might not read
     * any bytesProduced at all. Whether or not it does so depends upon the nature and
     * state of the underlying channel. It is guaranteed, however, that if a
     * channel is in blocking mode and there is at least one byte remaining in
     * the buffer then this method will block until at least one byte is read.
     * On the other hand, if the underlying channel is in non-blocking mode then
     * a {@link WouldBlockException} may be thrown. Note that this also includes
     * the possibility of a {@link NeedsWriteException}, due to the fact that,
     * during a TLS handshake, bytesProduced need to be written to the underlying
     * channel. In any case, after a {@link WouldBlockException}, the operation
     * should be retried when the underlying channel is ready (for reading or
     * writing, depending on the subclass).
     *
     * <p>
     * If the channel is configured to not run tasks and one is due to run, a
     * {@link NeedsTaskException} will be thrown. In this case the operation
     * should be retried after the task is run.
     *
     * <p>
     * This method may be invoked at any time. If another thread has already
     * initiated a read or handshaking operation upon this channel, however,
     * then an invocation of this method will block until the first operation is
     * complete.
     *
     * @param dst The buffer into which bytesProduced are to be transferred
     * @return The number of bytesProduced read, or <tt>-1</tt> if the channel has
     * reached end-of-stream; contrary to the behavior specified in
     * {@link ByteChannel}, this method never returns 0, but throws
     * {@link WouldBlockException}
     * @throws WouldBlockException if the channel is in non-blocking mode and the IO operation
     *                             cannot be completed immediately
     * @throws NeedsTaskException  if the channel is not configured to run tasks automatically
     *                             and a task needs to be executed to complete the operation
     * @throws SSLException        if the {@link SSLEngine} throws a SSLException
     * @throws IOException         if the underlying channel throws an IOException
     */
    int read(ByteBuffer dst) throws IOException;

    /**
     * Writes a sequence of bytesProduced to this channel from the given buffer.
     *
     * <p>
     * An attempt is made to write up to <i>r</i> bytesProduced to the channel, where
     * <i>r</i> is the number of bytesProduced remaining in the buffer, that is,
     * <tt>src.remaining()</tt>, at the moment this method is invoked.
     *
     * <p>
     * Suppose that a byte sequence of length <i>n</i> is written, where
     * <tt>0</tt>&nbsp;<tt>&lt;=</tt>&nbsp;<i>n</i>&nbsp;<tt>&lt;=</tt>&nbsp;
     * <i>r</i>. This byte sequence will be transferred from the buffer starting
     * at index <i>p</i>, where <i>p</i> is the buffer's position at the moment
     * this method is invoked; the index of the last byte written will be
     * <i>p</i>&nbsp;<tt>+</tt>&nbsp;<i>n</i>&nbsp;<tt>-</tt>&nbsp;<tt>1</tt>.
     * Upon return the buffer's position will be equal to <i>p</i>&nbsp;
     * <tt>+</tt>&nbsp;<i>n</i>; its limit will not have changed.
     *
     * <p>
     * If the underlying channel is in blocking mode, a write operation will
     * return only after writing all of the <i>r</i> requested bytesProduced. On the
     * other hand, if it is in non-blocking mode, this operation may write only
     * some of the bytesProduced or possibly none at all, in this case a
     * {@link WouldBlockException} will be thrown. Note that this also includes
     * the possibility of a {@link NeedsReadException}, due to the fact that,
     * during a TLS handshake, bytes need to be read from the underlying channel.
     * In any case, after a {@link WouldBlockException}, the operation should be
     * retried when the underlying channel is ready (for reading or writing,
     * depending on the subclass).
     *
     * <p>
     * If the channel is configured to not run tasks and one is due to run, a
     * {@link NeedsTaskException} will be thrown. In this case the operation
     * should be retried after the task is run.
     *
     * <p>
     * This method may be invoked at any time. If another thread has already
     * initiated a write or handshaking operation upon this channel, however,
     * then an invocation of this method will block until the first operation is
     * complete.
     *
     * @param src The buffer from which bytesProduced are to be retrieved
     * @return The number of bytesProduced written, contrary to the behavior specified
     * in {@link ByteChannel}, this method never returns 0, but throws
     * {@link WouldBlockException}
     * @throws WouldBlockException if the channel is in non-blocking mode and the IO operation
     *                             cannot be completed immediately
     * @throws NeedsTaskException  if the channel is not configured to run tasks automatically
     *                             and a task needs to be executed to complete the operation
     * @throws SSLException        if the {@link SSLEngine} throws a SSLException
     * @throws IOException         if the underlying channel throws an IOException
     */
    int write(ByteBuffer src) throws IOException;

    /**
     * Initiates a handshake (initial or renegotiation) on this channel. This
     * method is not needed for the initial handshake, as the
     * <code>read()</code> and <code>write()</code> methods will implicitly do
     * the initial handshake if needed.
     *
     * <p>
     * This method may block if the underlying channel if in blocking mode.
     *
     * <p>
     * Note that renegotiation is a problematic feature of the TLS protocol,
     * that should only be initiated at quiet point of the protocol.
     *
     * <p>
     * This method may block if the underlying channel is in blocking mode,
     * otherwise a {@link WouldBlockException} can be thrown. In this case the
     * operation should be retried when the underlying channel is ready (for
     * reading or writing, depending on the subclass).
     *
     * <p>
     * If the channel is configured to not run tasks and one is due to run, a
     * {@link NeedsTaskException} will be thrown, with a reference to the task.
     * In this case the operation should be retried after the task is run.
     *
     * <p>
     * This method may be invoked at any time. If another thread has already
     * initiated a read, write, or handshaking operation upon this channel,
     * however, then an invocation of this method will block until the first
     * operation is complete.
     *
     * @throws WouldBlockException if the channel is in non-blocking mode and the IO operation
     *                             cannot be completed immediately
     * @throws NeedsTaskException  if the channel is not configured to run tasks automatically
     *                             and a task needs to be executed to complete the operation
     * @throws SSLException        if the {@link SSLEngine} throws a SSLException
     * @throws IOException         if the underlying channel throws an IOException
     */
    void renegotiate() throws IOException;

    /**
     * Forces the initial TLS handshake. Calling this method is usually not
     * needed, as a handshake will happen automatically when doing the first
     * <code>read()</code> or <code>write()</code> operation. Calling this
     * method after the initial handshake has been done has no effect.
     *
     * <p>
     * This method may block if the underlying channel is in blocking mode,
     * otherwise a {@link WouldBlockException} can be thrown. In this case the
     * operation should be retried when the underlying channel is ready (for
     * reading or writing, depending on the subclass).
     *
     * <p>
     * If the channel is configured to not run tasks and one is due to run, a
     * {@link NeedsTaskException} will be thrown, with a reference to the task.
     * In this case the operation should be retried after the task is run.
     *
     * <p>
     * This method may be invoked at any time. If another thread has already
     * initiated a read, write, or handshaking operation upon this channel,
     * however, then an invocation of this method will block until the first
     * operation is complete.
     *
     * @throws WouldBlockException if the channel is in non-blocking mode and the IO operation
     *                             cannot be completed immediately
     * @throws NeedsTaskException  if the channel is not configured to run tasks automatically
     *                             and a task needs to be executed to complete the operation
     * @throws SSLException        if the {@link SSLEngine} throws a SSLException
     * @throws IOException         if the underlying channel throws an IOException
     */
    void handshake() throws IOException;

    /**
     * Writes a sequence of bytesProduced to this channel from a subsequence of the
     * given buffers.
     *
     * <p>
     * See {@link GatheringByteChannel#write(ByteBuffer[], int, int)} for more
     * details of the meaning of this signature.
     *
     * <p>
     * This method behaves slightly different than the interface specification,
     * with respect to non-blocking responses, see {@link #write(ByteBuffer)}
     * for more details.
     *
     * @param srcs   The buffers from which bytesProduced are to be retrieved
     * @param offset The offset within the buffer array of the first buffer from
     *               which bytesProduced are to be retrieved; must be non-negative and no
     *               larger than <tt>srcs.length</tt>
     * @param length The maximum number of buffers to be accessed; must be
     *               non-negative and no larger than <tt>srcs.length</tt>
     *               &nbsp;-&nbsp;<tt>offset</tt>
     * @return The number of bytesProduced written, contrary to the behavior specified
     * in {@link ByteChannel}, this method never returns 0, but throws
     * {@link WouldBlockException}
     * @throws IndexOutOfBoundsException If the preconditions on the <tt>offset</tt> and
     *                                   <tt>length</tt> parameters do not hold
     * @throws WouldBlockException       if the channel is in non-blocking mode and the IO operation
     *                                   cannot be completed immediately
     * @throws NeedsTaskException        if the channel is not configured to run tasks automatically
     *                                   and a task needs to be executed to complete the operation
     * @throws SSLException              if the {@link SSLEngine} throws a SSLException
     * @throws IOException               if the underlying channel throws an IOException
     */
    long write(ByteBuffer[] srcs, int offset, int length) throws IOException;

    /**
     * Writes a sequence of bytesProduced to this channel from the given buffers.
     *
     * <p>
     * An invocation of this method of the form <tt>c.write(srcs)</tt> behaves
     * in exactly the same manner as the invocation
     *
     * <blockquote>
     *
     * <pre>
     * c.write(srcs, 0, srcs.length);
     * </pre>
     *
     * </blockquote>
     * <p>
     * This method behaves slightly different than the interface specification,
     * with respect to non-blocking responses, see {@link #write(ByteBuffer)}
     * for more details.
     *
     * @param srcs The buffers from which bytesProduced are to be retrieved
     * @return The number of bytesProduced written, contrary to the behavior specified
     * in {@link ByteChannel}, this method never returns 0, but throws
     * {@link WouldBlockException}
     * @throws IndexOutOfBoundsException If the preconditions on the <tt>offset</tt> and
     *                                   <tt>length</tt> parameters do not hold
     * @throws WouldBlockException       if the channel is in non-blocking mode and the IO operation
     *                                   cannot be completed immediately
     * @throws NeedsTaskException        if the channel is not configured to run tasks automatically
     *                                   and a task needs to be executed to complete the operation
     * @throws SSLException              if the {@link SSLEngine} throws a SSLException
     * @throws IOException               if the underlying channel throws an IOExceptions
     */
    long write(ByteBuffer[] srcs) throws IOException;

    /**
     * Reads a sequence of bytesProduced from this channel into a subsequence of the
     * given buffers.
     *
     * <p>
     * See {@link ScatteringByteChannel#read(ByteBuffer[], int, int)} for more
     * details of the meaning of this signature.
     *
     * <p>
     * This method behaves slightly different than the interface specification,
     * with respect to non-blocking responses, see {@link #read(ByteBuffer)} for
     * more details.
     *
     * @param dsts   The buffers into which bytesProduced are to be transferred
     * @param offset The offset within the buffer array of the first buffer into
     *               which bytesProduced are to be transferred; must be non-negative and no
     *               larger than <tt>dsts.length</tt>
     * @param length The maximum number of buffers to be accessed; must be
     *               non-negative and no larger than <tt>dsts.length</tt>
     *               &nbsp;-&nbsp;<tt>offset</tt>
     * @return The number of bytesProduced read, or <tt>-1</tt> if the channel has
     * reached end-of-stream; contrary to the behavior specified in
     * {@link ByteChannel}, this method never returns 0, but throws
     * {@link WouldBlockException}
     * @throws IndexOutOfBoundsException If the preconditions on the <tt>offset</tt> and
     *                                   <tt>length</tt> parameters do not hold
     * @throws WouldBlockException       if the channel is in non-blocking mode and the IO operation
     *                                   cannot be completed immediately
     * @throws NeedsTaskException        if the channel is not configured to run tasks automatically
     *                                   and a task needs to be executed to complete the operation
     * @throws SSLException              if the {@link SSLEngine} throws a SSLException
     * @throws IOException               if the underlying channel throws an IOException
     */
    long read(ByteBuffer[] dsts, int offset, int length) throws IOException;

    /**
     * Reads a sequence of bytesProduced from this channel into the given buffers.
     *
     * <p>
     * An invocation of this method of the form <tt>c.read(dsts)</tt> behaves in
     * exactly the same manner as the invocation
     *
     * <blockquote>
     *
     * <pre>
     * c.read(dsts, 0, dsts.length);
     * </pre>
     *
     * </blockquote>
     *
     * <p>
     * This method behaves slightly different than the interface specification,
     * with respect to non-blocking responses, see {@link #read(ByteBuffer)} for
     * more details.
     *
     * @param dsts The buffers into which bytesProduced are to be transferred
     * @return The number of bytesProduced read, or <tt>-1</tt> if the channel has
     * reached end-of-stream; contrary to the behavior specified in
     * {@link ByteChannel}, this method never returns 0, but throws
     * {@link WouldBlockException}
     * @throws IndexOutOfBoundsException If the preconditions on the <tt>offset</tt> and
     *                                   <tt>length</tt> parameters do not hold
     * @throws WouldBlockException       if the channel is in non-blocking mode and the IO operation
     *                                   cannot be completed immediately
     * @throws NeedsTaskException        if the channel is not configured to run tasks automatically
     *                                   and a task needs to be executed to complete the operation
     * @throws SSLException              if the {@link SSLEngine} throws a SSLException
     * @throws IOException               if the underlying channel throws an IOException
     */
    long read(ByteBuffer[] dsts) throws IOException;

    /**
     * Closes the underlying channel. This method first does some form of TLS
     * close if not already done. The exact behavior can be configured using the
     * {@link TlsChannelBuilder#withWaitForCloseConfirmation}.
     *
     * <p>
     * The default behavior mimics what happens in a normal (that is, non
     * layered) {@link javax.net.ssl.SSLSocket#close()}.
     *
     * <p>
     * For finer control of the TLS close, use {@link #shutdown()}
     *
     * @throws IOException if the underlying channel throws an IOException during close.
     *                     Exceptions thrown during any previous TLS close are not
     *                     propagated.
     */
    void close() throws IOException;

    /**
     * <p> Shuts down the TLS connection. This method emulates the behavior of OpenSSL's <a
     * href="https://wiki.openssl.org/index.php/Manual:SSL_shutdown(3)"> SSL_shutdown()</a>.</p>
     *
     * <p> The shutdown procedure consists of two steps: the sending of the "close notify" shutdown alert and the
     * reception of the peer's "close notify". According to the TLS standard, it is acceptable for an application to
     * only send its shutdown alert and then close the underlying connection without waiting for the peer's response.
     * When the underlying connection shall be used for more communications, the complete shutdown procedure
     * (bidirectional "close notify" alerts) must be performed, so that the peers stay synchronized.</p>
     *
     * <p> This class supports both uni- and bidirectional shutdown by its 2 step behavior, using this method.</p>
     *
     * <p> When this is the first party to send the "close notify" alert, this method will only send the alert, set the
     * {@link #shutdownSent()} flag and return <code>false</code>. If a unidirectional shutdown is enough, this first
     * call is sufficient. In order to complete the bidirectional shutdown handshake, This method must be called again.
     * The second call will wait for the peer's "close notify" shutdown alert. On success, the second call will return
     * <code>true</code>.</p>
     *
     * <p> If the peer already sent the "close notify" alert and it was already processed implicitly inside a read
     * operation, the {@link #shutdownReceived()} flag is already set. This method will then send the "close notify"
     * alert, set the {@link #shutdownSent()} flag and immediately return <code>true</code>. It is therefore recommended
     * to check the return value of this method and call it again, if the bidirectional shutdown is not yet
     * complete.</p>
     *
     * <p> If the underlying channel is blocking, this method will only return once the handshake step has been finished
     * or an error occurred.</p>
     *
     * <p> If the underlying channel is non-blocking, this method may throw {@link WouldBlockException} if the
     * underlying channel could not support the continuation of the handshake. The calling process then must repeat the
     * call after taking appropriate action (like waiting in a selector in case of a {@link SocketChannel}).</p>
     *
     * <p> Note that despite not being mandated by the specification, a proper TLS close is important to prevent
     * truncation attacks, which consists, essentially, of an adversary introducing TCP FIN segments to trick on party
     * to ignore the final bytes of a secure stream. For more details, see <a href="https://hal.inria.fr/hal-01102013">the
     * original paper</a>.</p>
     *
     * @return whether the closing is finished.
     * @throws IOException         if the underlying channel throws an IOException
     * @throws WouldBlockException if the channel is in non-blocking mode and the IO operation cannot be completed
     *                             immediately
     * @see TlsChannelBuilder#withWaitForCloseConfirmation(boolean)
     */
    boolean shutdown() throws IOException;

    /**
     * Return whether this side of the connection has already received the close
     * notification.
     *
     * @return <code>true</code> if the close notification was received
     * @see #shutdown()
     */
    boolean shutdownReceived();

    /**
     * Return whether this side of the connection has already sent the close
     * notification.
     *
     * @return <code>true</code> if the close notification was sent
     * @see #shutdown()
     */
    boolean shutdownSent();

}
