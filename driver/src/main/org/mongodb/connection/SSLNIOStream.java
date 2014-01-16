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

package org.mongodb.connection;


import org.bson.ByteBuf;
import org.mongodb.operation.SingleResultFuture;

import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.util.List;
import java.util.concurrent.ExecutorService;

class SSLNIOStream implements Stream {
    private final ServerAddress serverAddress;
    private final BufferProvider bufferProvider;
    private SocketClient socketClient;
    private final ExecutorService executorService;

    public SSLNIOStream(final ServerAddress serverAddress, final BufferProvider bufferProvider, final ExecutorService service) {
        this.serverAddress = serverAddress;
        this.bufferProvider = bufferProvider;
        executorService = service;
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        SingleResultFuture<Void> future = new SingleResultFuture<Void>();
        writeAsync(buffers, new FutureAsyncCompletionHandler(future));
        future.get();
    }

    @Override
    public void read(final ByteBuf buffer) throws IOException {
        SingleResultFuture<Void> future = new SingleResultFuture<Void>();
        readAsync(buffer, new FutureAsyncCompletionHandler(future));
        future.get();
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler handler) {
        ensureOpen(new AsyncCompletionHandler() {
            @Override
            public void completed() {
                socketClient.write(buffers, new BasicCompletionHandler(handler));
            }

            @Override
            public void failed(final Throwable t) {
                handler.failed(t);
            }
        });
    }

    @Override
    public void readAsync(final ByteBuf buffer, final AsyncCompletionHandler handler) {
        socketClient.read(buffer.asNIO(), new BasicCompletionHandler(handler));
    }

    @Override
    public ServerAddress getAddress() {
        return serverAddress;
    }

    @Override
    public void close() {
        try {
            socketClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isClosed() {
        return socketClient == null || !socketClient.isConnected();
    }

    private static final class BasicCompletionHandler implements CompletionHandler<Void, Void> {
        private final AsyncCompletionHandler handler;

        private BasicCompletionHandler(final AsyncCompletionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void completed(final Void result, final Void attachment) {
            handler.completed();
        }

        @Override
        public void failed(final Throwable t, final Void attachment) {
            handler.failed(t);
        }
    }

    protected void ensureOpen(final AsyncCompletionHandler handler) {
        try {
            if (socketClient != null) {
                handler.completed();
            } else {
                socketClient = new SocketClient(serverAddress, executorService);
                socketClient.connect(handler);
            }
        } catch (IOException e) {
            throw new MongoSocketOpenException(e.getMessage(), serverAddress, e);
        }
    }
}