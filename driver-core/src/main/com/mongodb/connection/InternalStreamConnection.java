/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import org.bson.ByteBuf;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

class InternalStreamConnection implements InternalConnection {

    private final String clusterId;
    private final Stream stream;
    private final ConnectionListener connectionListener;
    private final StreamPipeline streamPipeline;
    private final ConnectionInitializer connectionInitializer;
    private volatile boolean opened;
    private volatile boolean closed;

    static final Logger LOGGER = Loggers.getLogger("connection");

    InternalStreamConnection(final String clusterId, final Stream stream, final ConnectionListener connectionListener,
                             final ConnectionInitializerFactory connectionInitializerFactory) {
        this.clusterId = notNull("clusterId", clusterId);
        this.stream = notNull("stream", stream);
        this.connectionListener = notNull("connectionListener", connectionListener);
        this.connectionInitializer = connectionInitializerFactory.create(this);
        this.streamPipeline = new StreamPipeline(clusterId, stream, connectionListener, this);
    }

    @Override
    public ServerAddress getServerAddress() {
        return stream.getAddress();
    }

    @Override
    public String getId() {
        return connectionInitializer.getId();
    }

    @Override
    public ConnectionDescription getDescription() {
        return connectionInitializer.getDescription();
    }

    @Override
    public void open() {
        try {
            connectionInitializer.initialize();
            opened = true;
            try {
                connectionListener.connectionOpened(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
            } catch (Throwable t) {
                LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", t);
            }
        } catch (Exception e) {
            close();
            if (e instanceof MongoException) {
                throw (MongoException) e;
            } else {
                throw new MongoException(e.toString());
            }
        }
    }

    @Override
    public MongoFuture<Void> openAsync() {
        final SingleResultFuture<Void> future = new SingleResultFuture<Void>();
        connectionInitializer.initializeAsync(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                if (e != null) {
                    close();
                    future.init(null, e);
                } else {
                    opened = true;
                    future.init(null, null);
                }
                try {
                    connectionListener.connectionOpened(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
                } catch (Throwable t) {
                    LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", t);
                }
            }
        });
        return future;
    }

    @Override
    public boolean isOpened() {
        return opened && !closed;
    }

    @Override
    public void close() {
        closed = true;
        stream.close();
        try {
            connectionListener.connectionClosed(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
        } catch (Throwable t) {
            LOGGER.warn("Exception when trying to signal connectionClosed to the connectionListener", t);
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return stream.getBuffer(size);
    }

    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        streamPipeline.sendMessage(byteBuffers, lastRequestId);
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        return streamPipeline.receiveMessage(responseTo);
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        streamPipeline.sendMessageAsync(byteBuffers, lastRequestId, callback);
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        streamPipeline.receiveMessageAsync(responseTo, callback);
    }
}
