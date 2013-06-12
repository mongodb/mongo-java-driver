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

package org.mongodb.connection.impl;

import org.bson.ByteBuf;
import org.mongodb.MongoCredential;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerAddress;

import java.util.ArrayList;
import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

class AuthenticatingConnection implements Connection {
    private final List<MongoCredential> credentialList;
    private final BufferProvider bufferProvider;
    private volatile Connection wrapped;
    private boolean authenticated;

    public AuthenticatingConnection(final Connection wrapped, final List<MongoCredential> credentialList,
                                    final BufferProvider bufferProvider) {
        this.wrapped = notNull("wrapped", wrapped);
        this.bufferProvider = notNull("bufferProvider", bufferProvider);

        notNull("credentialList", credentialList);
        this.credentialList = new ArrayList<MongoCredential>(credentialList);
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
        return wrapped == null;
    }

    @Override
    public ServerAddress getServerAddress() {
        isTrue("open", wrapped != null);
        return wrapped.getServerAddress();
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers) {
        isTrue("open", wrapped != null);
        authenticateAll();
        wrapped.sendMessage(byteBuffers);
    }

    @Override
    public ResponseBuffers receiveMessage(final ResponseSettings responseSettings) {
        isTrue("open", wrapped != null);
        authenticateAll();
        return wrapped.receiveMessage(responseSettings);
    }

    public void authenticateAll() {
        if (!authenticated) {
            for (MongoCredential cur : credentialList) {
                createAuthenticator(cur).authenticate();
            }
            authenticated = true;
        }
    }

    private Authenticator createAuthenticator(final MongoCredential credential) {
        Authenticator authenticator;
        if (credential.getMechanism().equals(MongoCredential.MONGODB_CR_MECHANISM)) {
            authenticator = new NativeAuthenticator(credential, wrapped, bufferProvider);
        }
        else if (credential.getMechanism().equals(MongoCredential.GSSAPI_MECHANISM)) {
            authenticator = new GSSAPIAuthenticator(credential, wrapped, bufferProvider);
        }
        else {
            throw new IllegalArgumentException("Unsupported authentication protocol: " + credential.getMechanism());
        }
        return authenticator;
    }
}
