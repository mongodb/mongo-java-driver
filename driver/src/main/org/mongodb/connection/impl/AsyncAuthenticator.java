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

import org.mongodb.CommandResult;
import org.mongodb.MongoCredential;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.SingleResultCallback;

abstract class AsyncAuthenticator {
    private final MongoCredential credential;
    private final AsyncConnection connection;

    AsyncAuthenticator(final MongoCredential credential, final AsyncConnection connection) {
        this.credential = credential;
        this.connection = connection;
    }

    MongoCredential getCredential() {
        return credential;
    }

    public AsyncConnection getConnection() {
        return connection;
    }

    abstract void authenticate(final SingleResultCallback<CommandResult> callback);
}
