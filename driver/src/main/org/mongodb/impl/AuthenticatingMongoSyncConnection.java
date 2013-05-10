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

import org.mongodb.MongoCredential;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;
import org.mongodb.io.CachingAuthenticator;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.io.ResponseBuffers;

import java.nio.ByteBuffer;
import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

class AuthenticatingMongoSyncConnection implements MongoSyncConnection {
    private volatile MongoSyncConnection wrapped;
    private final CachingAuthenticator authenticator;

    public AuthenticatingMongoSyncConnection(final MongoSyncConnection wrapped, final List<MongoCredential> credentialList,
                                             final BufferPool<ByteBuffer> bufferPool) {
        this.wrapped = notNull("wrapped", wrapped);
        this.authenticator = new CachingAuthenticator(new MongoCredentialsStore(credentialList), wrapped, bufferPool);
    }

    @Override
    public void close() {
        if (wrapped != null) {
            wrapped.close();
            wrapped = null;
        }
    }

    @Override
    public boolean isClosed() {
        return wrapped.isClosed();
    }

    @Override
    public ServerAddress getServerAddress() {
        isTrue("open", wrapped != null);
        return wrapped.getServerAddress();
    }

    @Override
    public void sendMessage(final ChannelAwareOutputBuffer buffer) {
        isTrue("open", wrapped != null);
        authenticator.authenticateAll();
        wrapped.sendMessage(buffer);
    }

    @Override
    public ResponseBuffers sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer) {
        isTrue("open", wrapped != null);
        authenticator.authenticateAll();
        return wrapped.sendAndReceiveMessage(buffer);
    }
}
