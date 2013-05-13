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

import org.mongodb.MongoClientOptions;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;

public class DefaultMongoServerStateNotifierFactory implements MongoServerStateNotifierFactory {
    private final BufferPool<ByteBuffer> bufferPool;
    private final MongoClientOptions options;
    private final MongoServerStateListener serverStateListener;

    public DefaultMongoServerStateNotifierFactory(final BufferPool<ByteBuffer> bufferPool, final MongoClientOptions options,
                                                  final MongoServerStateListener serverStateListener) {
        this.bufferPool = bufferPool;
        this.options = options;
        this.serverStateListener = serverStateListener;
    }


    @Override
    public MongoServerStateNotifier create(final ServerAddress serverAddress) {
        return new MongoIsMasterServerStateNotifier(serverStateListener,
                new DefaultMongoSyncConnectionFactory(options, serverAddress, bufferPool, null), bufferPool);
    }
}
