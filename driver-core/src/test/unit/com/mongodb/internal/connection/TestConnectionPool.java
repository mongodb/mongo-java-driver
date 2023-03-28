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

import com.mongodb.MongoException;
import com.mongodb.RequestContext;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestConnectionPool implements ConnectionPool {

    private final MongoException exceptionToThrow;
    private int generation;

    public TestConnectionPool() {
        exceptionToThrow = null;
    }

    @Override
    public InternalConnection get(final OperationContext operationContext) {
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
            public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext,
                    final RequestContext requestContext, final OperationContext operationContext) {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            @Override
            public <T> void send(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasMoreToCome() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder,
                    final SessionContext sessionContext, final RequestContext requestContext, final OperationContext operationContext,
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
            public ServerDescription getInitialServerDescription() {
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

            @Override
            public int getGeneration() {
                return 0;
            }
        };
    }

    @Override
    public InternalConnection get(final OperationContext operationContext, final long timeout, final TimeUnit timeUnit) {
        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }
        return get(operationContext);
    }

    @Override
    public void getAsync(final OperationContext operationContext, final SingleResultCallback<InternalConnection> callback) {
        if (exceptionToThrow != null) {
            callback.onResult(null, exceptionToThrow);
        } else {
            callback.onResult(get(new OperationContext()), null);
        }
    }

    @Override
    public void invalidate(@Nullable final Throwable cause) {
        generation++;
    }

    @Override
    public void invalidate(final ObjectId serviceId, final int generation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ready() {
    }

    @Override
    public void close() {
    }

    @Override
    public int getGeneration() {
       return generation;
    }
}
