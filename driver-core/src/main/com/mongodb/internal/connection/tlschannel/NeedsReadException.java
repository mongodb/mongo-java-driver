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

import java.nio.channels.ByteChannel;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * This exception signals the caller that the operation cannot continue because bytesProduced need
 * to be read from the underlying {@link ByteChannel}, the channel is non-blocking and there are no
 * bytesProduced available. The caller should try the operation again, either with the channel in
 * blocking mode of after ensuring that bytesProduced are ready.
 *
 * <p>For {@link SocketChannel}s, a {@link Selector} can be used to find out when the method should
 * be retried.
 *
 * <p>Caveat: Any {@link TlsChannel} I/O method can throw this exception. In particular, <code>write
 * </code> may want to read data. This is because TLS handshakes may occur at any time (initiated by
 * either the client or the server).
 *
 * <p>This exception is akin to the SSL_ERROR_WANT_READ error code used by OpenSSL.
 *
 * @see <a href="https://www.openssl.org/docs/man1.1.0/ssl/SSL_get_error.html">OpenSSL error
 *     documentation</a>
 */
public class NeedsReadException extends WouldBlockException {
  private static final long serialVersionUID = 1419735639675146947L;
}
