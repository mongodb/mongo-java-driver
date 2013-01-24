/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.impl;


import org.mongodb.MongoClient;
import org.mongodb.MongoClientOptions;
import org.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.List;

/**
 * THIS IS NOT PART OF THE PUBLIC API. It may change at any time without notice.
 */
public class MongoClientAdapter {

    private final AbstractMongoClient adapted;

    public MongoClientAdapter(final ServerAddress serverAddress, final MongoClientOptions options) {
        adapted = MongoClientsImpl.create(serverAddress, options);
    }

    public MongoClientAdapter(final List<ServerAddress> seedList, final MongoClientOptions options) {
        adapted = MongoClientsImpl.create(seedList, options);
    }

    public MongoClientAdapter(final org.mongodb.MongoClientURI mongoURI) throws UnknownHostException {
        adapted = MongoClientsImpl.create(mongoURI);
    }

    public DBAdapter getDB(final String name) {
        return new DBAdapter(adapted.getDatabase(name));
    }

    public MongoClient getClient() {
        return adapted;
    }

    public List<ServerAddress> getServerAddressList() {
        return adapted.getServerAddressList();
    }

    public void bindToConnection() {
        adapted.bindToConnection();
    }

    public void unbindFromConnection() {
        adapted.unbindFromConnection();
    }
}
