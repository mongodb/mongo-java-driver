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

// TODO: take this private.  Currently only used by a test
public abstract class Authenticator {
    private final MongoCredential credential;
    private final Connection connection;

    Authenticator(final MongoCredential credential, final Connection connection) {
        this.credential = credential;
        this.connection = connection;
    }

    MongoCredential getCredential() {
        return credential;
    }

    public Connection getConnection() {
        return connection;
    }

    abstract void authenticate();
}
