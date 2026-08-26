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
    private final ServerAddress serverAddress;
    private final long timeoutMillis;

    /**
     * Construct a new instance.
     *
     * @param serverAddress the address of the KMS host, which may not be null
     * @param timeoutMillis the time available to establish the connection, which must be positive
     */
    public KmsConnectContext(final ServerAddress serverAddress, final long timeoutMillis) {
        this.serverAddress = notNull("serverAddress", serverAddress);
        isTrueArgument("timeoutMillis > 0", timeoutMillis > 0);
        this.timeoutMillis = timeoutMillis;
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
     * <p>Implementations are encouraged to honor it, so that a connection attempt does not outlive the time the driver
     * has allotted to it.</p>
     *
     * @return the time available in milliseconds, always positive
     */
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public String toString() {
        return "KmsConnectContext{"
                + "serverAddress=" + serverAddress
                + ", timeoutMillis=" + timeoutMillis
                + '}';
    }
}
