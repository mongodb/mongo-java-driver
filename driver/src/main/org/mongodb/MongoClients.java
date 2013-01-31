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

package org.mongodb;

import org.mongodb.annotations.ThreadSafe;
import org.mongodb.impl.MongoClientsImpl;

import java.net.UnknownHostException;
import java.util.List;

@ThreadSafe
public final class MongoClients {
    private MongoClients() {
    }

    public static MongoClient create(final ServerAddress serverAddress) {
        return MongoClientsImpl.create(serverAddress, null);
    }

    public static MongoClient create(final ServerAddress serverAddress, final MongoClientOptions options) {
        return MongoClientsImpl.create(serverAddress, options);
    }

    public static MongoClient create(final List<ServerAddress> serverAddresses) {
        return MongoClientsImpl.create(serverAddresses, null);
    }

    public static MongoClient create(final List<ServerAddress> serverAddresses, final MongoClientOptions options) {
        return MongoClientsImpl.create(serverAddresses, options);
    }

    public static MongoClient create(final MongoClientURI mongoURI) throws UnknownHostException {
        return MongoClientsImpl.create(mongoURI);
    }
}
