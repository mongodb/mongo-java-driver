/*
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

package com.mongodb;

import org.mongodb.MongoClient;
import org.mongodb.MongoClients;

import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Mongo {
    private final MongoClient client;
    private final ConcurrentMap<String,DB> dbCache = new ConcurrentHashMap<String,DB>();
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;

    public Mongo() throws UnknownHostException {
        this(new ServerAddress());
    }

    public Mongo(final ServerAddress serverAddress) {
        client = MongoClients.create(serverAddress.toNew());
    }


    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public DB getDB(String dbName) {
        DB db = dbCache.get(dbName);
        if (db != null)
            return db;

        db = new DB(this, dbName);
        DB temp = dbCache.putIfAbsent(dbName, db);
        if (temp != null)
            return temp;
        return db;
    }

    MongoClient getNew() {
        return client;
    }
}
