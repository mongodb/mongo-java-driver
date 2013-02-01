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

package org.mongodb.impl;

import org.bson.util.BufferPool;
import org.mongodb.ClientAdmin;
import org.mongodb.MongoClientOptions;
import org.mongodb.ServerAddress;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

abstract class SingleChannelMongoClient extends AbstractMongoClient {
    private final ServerAddress serverAddress;

    SingleChannelMongoClient(final ServerAddress serverAddress, final BufferPool<ByteBuffer> bufferPool,
                             final MongoClientOptions options) {
        super(options, bufferPool);
        this.serverAddress = serverAddress;
    }

    @Override
    public void withConnection(final Runnable runnable) {
        runnable.run();
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public ClientAdmin tools() {
        return new ClientAdminImpl(this.getOperations(), getOptions().getPrimitiveSerializers());
    }

    @Override
    void bindToConnection() {
    }

    @Override
    void unbindFromConnection() {
    }

    @Override
    List<ServerAddress> getServerAddressList() {
        return Arrays.asList(serverAddress);
    }
}
