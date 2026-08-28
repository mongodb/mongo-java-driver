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

package com.mongodb;

import com.mongodb.annotations.Immutable;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * The details of a Key Management Service (KMS) connection that a {@link KmsConnectCallback} is asked to establish.
 *
 * @see KmsConnectCallback
 * @since 5.11
 */
@Immutable
public final class KmsConnectContext {
    private final String host;
    private final int port;

    /**
     * Construct a new instance.
     *
     * @param host the host name of the KMS host, which may not be null
     * @param port the port of the KMS host, which must be positive
     */
    public KmsConnectContext(final String host, final int port) {
        this.host = notNull("host", host);
        isTrueArgument("port > 0", port > 0);
        this.port = port;
    }

    /**
     * Gets the host name of the KMS host that the connection must ultimately reach.
     *
     * <p>This is not the host name of any intermediary such as a proxy. It is the host that the driver negotiates TLS
     * with once the callback returns.</p>
     *
     * @return the host name of the KMS host, never null
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port of the KMS host that the connection must ultimately reach.
     *
     * <p>This is not the port of any intermediary such as a proxy.</p>
     *
     * @return the port of the KMS host, always positive
     */
    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "KmsConnectContext{"
                + "host='" + host + '\''
                + ", port=" + port
                + '}';
    }
}
