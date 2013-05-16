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
import org.mongodb.operation.CommandResult;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

class CachingAsyncAuthenticator {
    private final MongoCredentialsStore credentialsStore;
    private final AsyncConnection connection;
    private BufferPool<ByteBuffer> bufferPool;
    // needs synchronization to ensure that modifications are published.
    private final Set<String> authenticatedDatabases = Collections.synchronizedSet(new HashSet<String>());

    public CachingAsyncAuthenticator(final MongoCredentialsStore credentialsStore, final AsyncConnection connection,
                                     final BufferPool<ByteBuffer> bufferPool) {
        this.credentialsStore = credentialsStore;
        this.connection = connection;
        this.bufferPool = bufferPool;
    }

    public void asyncAuthenticateAll(final SingleResultCallback<Void> callback) {
        new IteratingAuthenticator(callback).start();
    }

    // get the difference between the set of credentialed databases and the set of authenticated databases on this connection
    public Set<String> getUnauthenticatedDatabases() {
        Set<String> unauthenticatedDatabases = new HashSet<String>(credentialsStore.getDatabases());
        unauthenticatedDatabases.removeAll(authenticatedDatabases);
        return unauthenticatedDatabases;
    }

    private final class IteratingAuthenticator implements SingleResultCallback<CommandResult> {
        private final SingleResultCallback<Void> callback;
        private volatile String curDatabaseName;
        private final Iterator<String> iter;

        private IteratingAuthenticator(final SingleResultCallback<Void> callback) {
            this.callback = callback;
            iter = getUnauthenticatedDatabases().iterator();
        }

        private void start() {
            next();
        }

        private void next() {
            if (!iter.hasNext()) {
                callback.onResult(null, null);
            }
            else {
                curDatabaseName = iter.next();
                asyncAuthenticate(credentialsStore.get(curDatabaseName), this);
            }
        }

        @Override
        public void onResult(final CommandResult result, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            }
            else {
                authenticatedDatabases.add(curDatabaseName);
                next();
            }
        }
    }

    private void asyncAuthenticate(final MongoCredential credential, final SingleResultCallback<CommandResult> callback) {
        createAuthenticator(credential).authenticate(callback);
    }

    private AsyncAuthenticator createAuthenticator(final MongoCredential credential) {
        AsyncAuthenticator authenticator;
        if (credential.getMechanism().equals(MongoCredential.MONGODB_CR_MECHANISM)) {
            authenticator = new NativeAsyncAuthenticator(credential, connection, bufferPool);
        }
        else if (credential.getMechanism().equals(MongoCredential.GSSAPI_MECHANISM)) {
            authenticator = new GSSAPIAsyncAuthenticator(credential, connection, bufferPool);
        }
        else {
            throw new IllegalArgumentException("Unsupported authentication protocol: " + credential.getMechanism());
        }
        return authenticator;
    }
}
