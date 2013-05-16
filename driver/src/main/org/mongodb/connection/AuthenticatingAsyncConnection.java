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

package org.mongodb.connection;

import org.mongodb.MongoCredential;
import org.mongodb.MongoException;

import java.nio.ByteBuffer;
import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;

class AuthenticatingAsyncConnection implements AsyncConnection {

    private final CachingAsyncAuthenticator authenticator;
    private volatile AsyncConnection wrapped;

    public AuthenticatingAsyncConnection(final AsyncConnection wrapped, final List<MongoCredential> credentialList,
                                         final BufferPool<ByteBuffer> bufferPool) {
        this.wrapped = wrapped;
        authenticator = new CachingAsyncAuthenticator(new MongoCredentialsStore(credentialList), wrapped, bufferPool);
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
    public void sendMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        authenticator.asyncAuthenticateAll(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                if (e != null) {
                    callback.onResult(null, e);
                }
                else {
                    wrapped.sendMessage(buffer, callback);
                }
            }
        });
    }

    @Override
    public void sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        authenticator.asyncAuthenticateAll(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                if (e != null) {
                    callback.onResult(null, e);
                }
                else {
                    wrapped.sendAndReceiveMessage(buffer, callback);
                }
            }
        });
    }

    @Override
    public void receiveMessage(final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        authenticator.asyncAuthenticateAll(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                if (e != null) {
                    callback.onResult(null, e);
                }
                else {
                    wrapped.receiveMessage(callback);
                }
            }
        });
    }
}
