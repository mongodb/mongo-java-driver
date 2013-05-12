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
import org.mongodb.io.PowerOfTwoByteBufferPool;

import java.nio.ByteBuffer;

class MongoConnectionIsMasterExecutorFactory implements IsMasterExecutorFactory {

    private final MongoClientOptions options;
    private final BufferPool<ByteBuffer> bufferPool = new PowerOfTwoByteBufferPool();

    MongoConnectionIsMasterExecutorFactory(final MongoClientOptions options) {
        this.options = options;
    }

    @Override
    public IsMasterExecutor create(final ServerAddress serverAddress) {

        return new MongoConnectionIsMasterExecutor(new DefaultMongoSyncConnectionFactory(options, serverAddress, bufferPool, null),
                bufferPool);
    }
}
