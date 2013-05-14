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

import org.mongodb.MongoAsyncConnectionFactory;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;
import org.mongodb.MongoSyncConnectionFactory;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public final class Servers {
    // TODO: this is exposing the implementation class.  But I didn't want to make some of the methods in Server public, so
    // leaving this way for now.
    static DefaultServer create(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                              final MongoClientOptions options, final ScheduledExecutorService scheduledExecutorService,
                              final BufferPool<ByteBuffer> bufferPool) {
        MongoSyncConnectionFactory connectionFactory = new DefaultMongoSyncConnectionFactory(options,
                serverAddress, bufferPool, credentialList);
        MongoAsyncConnectionFactory asyncConnectionFactory = null;

        if (options.isAsyncEnabled() && !options.isSSLEnabled() && !System.getProperty("org.mongodb.useSocket", "false").equals("true")) {
            asyncConnectionFactory = new DefaultMongoAsyncConnectionFactory(options, serverAddress,
                    bufferPool, credentialList);
        }
        return new DefaultServer(serverAddress, connectionFactory, asyncConnectionFactory, options, scheduledExecutorService,
                bufferPool);
    }

    private Servers() {
    }
}
