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
import com.mongodb.async.SingleResultCallback;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestConnectionPool implements ConnectionPool {

    private final MongoException exceptionToThrow;

    public TestConnectionPool() {
        exceptionToThrow = null;
    }

    public TestConnectionPool(final MongoException exceptionToThrow) {
        this.exceptionToThrow = exceptionToThrow;
    }

    @Override
    public InternalConnection get() {
        return new InternalConnection() {
            @Override
            public ByteBuf getBuffer(final int capacity) {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            @Override
            public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            @Override
            public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder) {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            @Override
            public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder,
                                            final SingleResultCallback<T> callback) {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            @Override
            public ResponseBuffers receiveMessage(final int responseTo) {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            @Override
            public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId,
                                         final SingleResultCallback<Void> callback) {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            @Override
            public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            @Override
            public ConnectionDescription getDescription() {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            public void open() {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            public void openAsync(final SingleResultCallback<Void> callback) {
                callback.onResult(null, new UnsupportedOperationException("Not implemented yet"));
            }

            @Override
            public void close() {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            @Override
            public boolean opened() {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            @Override
            public boolean isClosed() {
                throw new UnsupportedOperationException("Not implemented yet!");
            }
        };
    }

    @Override
    public InternalConnection get(final long timeout, final TimeUnit timeUnit) {
        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }
        return get();
    }

    @Override
    public void getAsync(final SingleResultCallback<InternalConnection> callback) {
        if (exceptionToThrow != null) {
            callback.onResult(null, exceptionToThrow);
        } else {
            callback.onResult(get(), null);
        }
    }

    @Override
    public void invalidate() {
    }

    @Override
    public void close() {
    }
}
