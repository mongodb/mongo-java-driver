/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection.impl;

import org.bson.ByteBuf;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerAddress;

import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;

/**
 * A connection that tracks when it was opened and when it was last used.
 */
class UsageTrackingConnection implements Connection {
    private final long openedAt;
    private volatile long lastUsedAt;
    private final int generation;
    private volatile Connection wrapped;

    UsageTrackingConnection(final Connection wrapped, final int generation) {
        this.wrapped = wrapped;
        this.generation = generation;
        openedAt = System.currentTimeMillis();
        lastUsedAt = openedAt;
    }

    @Override
    public void close() {
        wrapped.close();
        wrapped = null;
    }

    @Override
    public boolean isClosed() {
        return wrapped == null || wrapped.isClosed();
    }

    @Override
    public ServerAddress getServerAddress() {
        isTrue("open", !isClosed());
        return wrapped.getServerAddress();
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers) {
        wrapped.sendMessage(byteBuffers);
        lastUsedAt = System.currentTimeMillis();
    }

    @Override
    public ResponseBuffers receiveMessage() {
        ResponseBuffers responseBuffers = wrapped.receiveMessage();
        lastUsedAt = System.currentTimeMillis();
        return responseBuffers;
    }

    /**
     * Gets the generation of this connection.  This can be used by connection pools to track whether the connection is stale.
     *
     * @return the generation.
     */
    int getGeneration() {
        return generation;
    }

    /**
     * Returns the time at which this connection was opened, or {@code Long.MAX_VALUE} if it has not yet been opened.
     *
     * @return the time when this connection was opened, in milliseconds since the epoch.
     */
    long getOpenedAt() {
        return openedAt;
    }

    /**
     * Returns the time at which this connection was last used, or {@code Long.MAX_VALUE} if it has not yet been used.
     *
     * @return the time when this connection was last used, in milliseconds since the epoch.
     */
    long getLastUsedAt() {
        return lastUsedAt;
    }
}
