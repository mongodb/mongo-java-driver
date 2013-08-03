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
import org.mongodb.CommandResult;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SingleResultCallback;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

class AuthenticatingAsyncConnection implements AsyncConnection {
    private final List<MongoCredential> credentialList;
    private final BufferProvider bufferProvider;
    private volatile AsyncConnection wrapped;
    private volatile boolean authenticated;

    public AuthenticatingAsyncConnection(final AsyncConnection wrapped, final List<MongoCredential> credentialList,
                                         final BufferProvider bufferProvider) {
        this.wrapped = notNull("wrapped", wrapped);
        this.bufferProvider = notNull("bufferProvider", bufferProvider);

        notNull("credentialList", credentialList);
        this.credentialList = new ArrayList<MongoCredential>(credentialList);
    }

    @Override
    public void close() {
        try {
            wrapped.close();
        } finally {
            wrapped = null;
        }
    }

    @Override
    public boolean isClosed() {
        return wrapped == null;
    }

    @Override
    public ServerAddress getServerAddress() {
        isTrue("open", !isClosed());
        return wrapped.getServerAddress();
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        isTrue("open", !isClosed());
        authenticateAll(new AuthenticationCallback<Void>(callback) {
            @Override
            protected void proceed() {
                wrapped.sendMessage(byteBuffers, callback);
            }
        });
    }

    @Override
    public void receiveMessage(final ResponseSettings responseSettings, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        authenticateAll(new AuthenticationCallback<ResponseBuffers>(callback) {
            @Override
            protected void proceed() {
                wrapped.receiveMessage(responseSettings, callback);
            }
        });
    }

    private void authenticateAll(final SingleResultCallback<Void> callback) {
        if (authenticated) {
            callback.onResult(null, null);
        }
        else {
            new IteratingAuthenticator(callback).start();
        }
    }

    private final class IteratingAuthenticator implements SingleResultCallback<CommandResult> {
        private final SingleResultCallback<Void> callback;
        private volatile MongoCredential curCredential;
        private final Iterator<MongoCredential> iter;

        private IteratingAuthenticator(final SingleResultCallback<Void> callback) {
            this.callback = callback;
            iter = credentialList.iterator();
        }

        private void start() {
            next();
        }

        private void next() {
            if (!iter.hasNext()) {
                callback.onResult(null, null);
            }
            else {
                curCredential = iter.next();
                createAuthenticator(curCredential).authenticate(this);
            }
        }

        @Override
        public void onResult(final CommandResult result, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            }
            else {
                next();
            }
        }

        private AsyncAuthenticator createAuthenticator(final MongoCredential credential) {
            switch (credential.getMechanism()) {
                case MONGODB_CR:
                    return new NativeAsyncAuthenticator(credential, wrapped, bufferProvider);
                case GSSAPI:
                    return new GSSAPIAsyncAuthenticator(credential, wrapped, bufferProvider);
                case PLAIN:
                    return new PlainAsyncAuthenticator(credential, wrapped, bufferProvider);
                default:
                    throw new IllegalArgumentException("Unsupported authentication protocol: " + credential.getMechanism());
            }
        }
    }

    private abstract class AuthenticationCallback<T> implements SingleResultCallback<Void> {
        private final SingleResultCallback<T> callback;

        public AuthenticationCallback(final SingleResultCallback<T> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(final Void result, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            }
            else {
                authenticated = true;
                proceed();
            }
        }

        protected abstract void proceed();
    }
}
