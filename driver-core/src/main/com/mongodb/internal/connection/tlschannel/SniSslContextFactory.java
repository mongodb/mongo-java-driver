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

import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import java.util.Optional;

/**
 * Factory for {@link SSLContext}s, based in an optional {@link SNIServerName}. Implementations of
 * this interface are supplied to {@link ServerTlsChannel} instances, to select the correct context
 * (and so the correct certificate) based on the server name provided by the client.
 */
@FunctionalInterface
public interface SniSslContextFactory {

  /**
   * Return a proper {@link SSLContext}.
   *
   * @param sniServerName an optional {@link SNIServerName}; an empty value means that the client
   *     did not send and SNI value.
   * @return the chosen context, or an empty value, indicating that no context is supplied and the
   *     connection should be aborted.
   */
  Optional<SSLContext> getSslContext(Optional<SNIServerName> sniServerName);
}
