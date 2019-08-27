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

package com.mongodb.internal.connection;

import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.session.SessionContext;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;

import java.util.List;

import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;

/**
 * A connection that tracks when it was opened and when it was last used.
 */
class UsageTrackingInternalConnection implements InternalConnection {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private volatile long openedAt;
    private volatile long lastUsedAt;
    private final int generation;
    private final InternalConnection wrapped;

    UsageTrackingInternalConnection(final InternalConnection wrapped, final int generation) {
        this.wrapped = wrapped;
        this.generation = generation;
        openedAt = Long.MAX_VALUE;
        lastUsedAt = openedAt;
    }

    @Override
    public void open() {
        wrapped.open();
        openedAt = System.currentTimeMillis();
        lastUsedAt = openedAt;
    }

    @Override
    public void openAsync(final SingleResultCallback<Void> callback) {
        wrapped.openAsync(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    openedAt = System.currentTimeMillis();
                    lastUsedAt = openedAt;
                    callback.onResult(null, null);
                }
            }
        });
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public boolean opened() {
        return wrapped.opened();
    }

    @Override
    public boolean isClosed() {
        return wrapped.isClosed();
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return wrapped.getBuffer(size);
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        wrapped.sendMessage(byteBuffers, lastRequestId);
        lastUsedAt = System.currentTimeMillis();
    }

    @Override
    public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
        T result = wrapped.sendAndReceive(message, decoder, sessionContext);
        lastUsedAt = System.currentTimeMillis();
        return result;
    }

    @Override
    public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder,
                                        final SessionContext sessionContext, final SingleResultCallback<T> callback) {
        SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final Throwable t) {
                lastUsedAt = System.currentTimeMillis();
                callback.onResult(result, t);
            }
        }, LOGGER);
        wrapped.sendAndReceiveAsync(message, decoder, sessionContext, errHandlingCallback);
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        ResponseBuffers responseBuffers = wrapped.receiveMessage(responseTo);
        lastUsedAt = System.currentTimeMillis();
        return responseBuffers;
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                lastUsedAt = System.currentTimeMillis();
                callback.onResult(result, t);
            }
        }, LOGGER);
        wrapped.sendMessageAsync(byteBuffers, lastRequestId, errHandlingCallback);
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        SingleResultCallback<ResponseBuffers> errHandlingCallback = errorHandlingCallback(new SingleResultCallback<ResponseBuffers>() {
            @Override
            public void onResult(final ResponseBuffers result, final Throwable t) {
                lastUsedAt = System.currentTimeMillis();
                callback.onResult(result, t);
            }
        }, LOGGER);
        wrapped.receiveMessageAsync(responseTo, errHandlingCallback);
    }

    @Override
    public ConnectionDescription getDescription() {
        return wrapped.getDescription();
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
