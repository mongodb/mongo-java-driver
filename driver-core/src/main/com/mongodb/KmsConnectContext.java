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
 * <p>Instances are created by the driver. Further properties may be added in future releases, so implementations of
 * {@link KmsConnectCallback} must not assume that this type is complete.</p>
 *
 * @see KmsConnectCallback
 * @since 5.11
 */
@Immutable
public final class KmsConnectContext {
    private final String kmsProvider;
    private final ServerAddress serverAddress;
    private final long timeoutMillis;

    /**
     * This constructor is not part of the public API and may be removed or changed at any time.
     *
     * @param kmsProvider the KMS provider
     * @param serverAddress the address of the KMS host
     * @param timeoutMillis the remaining time available, or 0 if no time limit applies
     */
    public KmsConnectContext(final String kmsProvider, final ServerAddress serverAddress, final long timeoutMillis) {
        this.kmsProvider = notNull("kmsProvider", kmsProvider);
        this.serverAddress = notNull("serverAddress", serverAddress);
        isTrueArgument("timeoutMillis >= 0", timeoutMillis >= 0);
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Gets the KMS provider that this connection is being established for.
     *
     * <p>This is one of the keys of the configured {@code kmsProviders} map, so either a provider name such as
     * {@code "aws"} or a named provider such as {@code "aws:myname"}. Implementations must accept arbitrary values for
     * forward compatibility: rather than rejecting an unrecognized provider, connect using a default route.</p>
     *
     * @return the KMS provider, never null
     */
    public String getKmsProvider() {
        return kmsProvider;
    }

    /**
     * Gets the address of the KMS host that the connection must ultimately reach.
     *
     * <p>This is not the address of any intermediary such as a proxy. It is the address that the driver negotiates TLS
     * with once the callback returns.</p>
     *
     * @return the address of the KMS host, never null
     */
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    /**
     * Gets the time available to establish the connection, in milliseconds.
     *
     * <p>When a timeout is configured this is the time remaining in the operation, so it shrinks as the operation
     * progresses. It is {@code 0} when no time limit applies.</p>
     *
     * @return the time available in milliseconds, or 0 if no time limit applies
     */
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public String toString() {
        return "KmsConnectContext{"
                + "kmsProvider='" + kmsProvider + '\''
                + ", serverAddress=" + serverAddress
                + ", timeoutMillis=" + timeoutMillis
                + '}';
    }
}
