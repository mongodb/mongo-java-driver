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

package com.mongodb.connection;

import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * An immutable connection identifier of a connection to a MongoDB server.
 *
 * <p>Contains a locally created id and if available the MongoDB server created connection id</p>
 *
 * @since 3.0
 */
@Immutable
public final class ConnectionId {
    private static final AtomicLong INCREMENTING_ID = new AtomicLong();

    private final ServerId serverId;
    private final long localValue;
    @Nullable
    private final Long serverValue;
    private final String stringValue;

    /**
     * Construct an instance with the given server id.
     *
     * @param serverId the server id
     * @since 3.8
     */
    public ConnectionId(final ServerId serverId) {
        this(serverId, INCREMENTING_ID.incrementAndGet(), null);
    }

    /**
     * Construct an instance with the given serverId, localValue, and serverValue.
     *
     * <p>
     *     Useful for testing, but generally prefer {@link #withServerValue(long)}
     * </p>
     *
     * @param serverId the server id
     * @param localValue the local value
     * @param serverValue the server value, which may be null
     * @see #withServerValue(long)
     * @since 3.11
     */
    public ConnectionId(final ServerId serverId, final long localValue, @Nullable final Long serverValue) {
        this.serverId = notNull("serverId", serverId);
        this.localValue = localValue;
        this.serverValue = serverValue;
        if (serverValue == null) {
            stringValue = format("connectionId{localValue:%s}", localValue);
        } else {
            stringValue = format("connectionId{localValue:%s, serverValue:%s}", localValue, serverValue);
        }
    }

    /**
     * Creates a new connectionId with the set server value
     *
     * @param serverValue the server value
     * @return the new connection id
     * @since 3.8
     */
    public ConnectionId withServerValue(final long serverValue) {
        isTrue("server value is null", this.serverValue == null);
        return new ConnectionId(serverId, localValue, serverValue);
    }

    /**
     * Gets the server id.
     *
     * @return the server id
     */
    public ServerId getServerId() {
        return serverId;
    }

    /**
     * Gets the locally created id value for the connection
     *
     * @return the locally created id value for the connection
     */
    public long getLocalValue() {
        return localValue;
    }

    /**
     * Gets the server generated id value for the connection or null if not set.
     *
     * @return the server generated id value for the connection or null if not set.
     */
    @Nullable
    public Long getServerValue() {
        return serverValue;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectionId that = (ConnectionId) o;

        if (localValue != that.localValue) {
            return false;
        }
        if (!serverId.equals(that.serverId)) {
            return false;
        }
        if (!Objects.equals(serverValue, that.serverValue)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId, localValue, serverValue);
    }

    @Override
    public String toString() {
        return stringValue;
    }
}
