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

package com.mongodb.event;

import com.mongodb.connection.ConnectionId;
import org.bson.BsonDocument;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for successful completion of a server heartbeat.
 *
 * @since 3.3
 */
public final class ServerHeartbeatSucceededEvent {
    private final ConnectionId connectionId;
    private final BsonDocument reply;
    private final long elapsedTimeNanos;
    private final boolean awaited;

    /**
     * Construct an instance.
     *
     * @param connectionId the non-null connectionId
     * @param reply the non-null reply to an hello command
     * @param elapsedTimeNanos the non-negative elapsed time in nanoseconds
     * @deprecated Prefer {@link #ServerHeartbeatSucceededEvent(ConnectionId, BsonDocument, long, boolean)}
     */
    @Deprecated
    public ServerHeartbeatSucceededEvent(final ConnectionId connectionId, final BsonDocument reply, final long elapsedTimeNanos) {
        this(connectionId, reply, elapsedTimeNanos, false);
    }

    /**
     * Construct an instance.
     *
     * @param connectionId the non-null connectionId
     * @param reply the non-null reply to an hello command
     * @param elapsedTimeNanos the non-negative elapsed time in nanoseconds
     * @param awaited true if the response was awaited
     * @since 4.1
     */
    public ServerHeartbeatSucceededEvent(final ConnectionId connectionId, final BsonDocument reply, final long elapsedTimeNanos,
                                         final boolean awaited) {
        this.connectionId = notNull("connectionId", connectionId);
        this.reply = notNull("reply", reply);
        isTrueArgument("elapsed time is not negative", elapsedTimeNanos >= 0);
        this.elapsedTimeNanos = elapsedTimeNanos;
        this.awaited = awaited;
    }

    /**
     * Gets the connectionId.
     *
     * @return the connectionId
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    /**
     * Gets the reply to the hello command executed for this heartbeat.
     *
     * @return the reply
     */
    public BsonDocument getReply() {
        return reply;
    }

    /**
     * Gets the elapsed time in the given time unit.
     *
     * @param timeUnit the non-null timeUnit
     *
     * @return the elapsed time in the given time unit
     */
    public long getElapsedTime(final TimeUnit timeUnit) {
        return timeUnit.convert(elapsedTimeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Gets whether the heartbeat was awaited.  If true, then {@link #getElapsedTime(TimeUnit)} reflects the sum of the round trip time
     * to the server and the time that the server waited before sending a response.
     *
     * @return whether the response was awaited
     * @since 4.1
     * @mongodb.server.release 4.4
     */
    public boolean isAwaited() {
        return awaited;
    }

    @Override
    public String toString() {
        return "ServerHeartbeatSucceededEvent{"
                + "connectionId=" + connectionId
                + ", server=" + connectionId.getServerId().getAddress()
                + ", clusterId=" + connectionId.getServerId().getClusterId()
                + ", reply=" + reply
                + ", elapsedTimeNanos=" + elapsedTimeNanos
                + ", awaited=" + awaited
                + "} ";
    }
}
