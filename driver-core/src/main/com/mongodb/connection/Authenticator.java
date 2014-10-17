/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

abstract class Authenticator {
    private final MongoCredential credential;
    private final InternalConnection internalConnection;

    Authenticator(final MongoCredential credential, final InternalConnection internalConnection) {
        this.credential = credential;
        this.internalConnection = internalConnection;
    }

    MongoCredential getCredential() {
        return credential;
    }

    InternalConnection getInternalConnection() {
        return internalConnection;
    }

    ServerAddress getServerAddress() {
        return internalConnection.getDescription().getServerAddress();
    }

    abstract void authenticate();
}
