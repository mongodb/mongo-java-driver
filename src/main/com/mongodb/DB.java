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

import org.mongodb.MongoDatabase;
import org.mongodb.serialization.PrimitiveSerializers;

import java.util.concurrent.ConcurrentHashMap;

public class DB {
    private final Mongo mongo;
    private final MongoDatabase database;
    private final ConcurrentHashMap<String, DBCollection> collectionCache = new ConcurrentHashMap<String, DBCollection>();
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;

    DB(final Mongo mongo, final String dbName) {
        this.mongo = mongo;
        database = mongo.getNew().getDatabase(dbName);
    }

    public ReadPreference getReadPreference() {
        return readPreference != null ? readPreference : mongo.getReadPreference();
    }

    public WriteConcern getWriteConcern() {
        return writeConcern != null ? writeConcern : mongo.getWriteConcern();
    }

    /**
     * starts a new "consistent request".
     * Following this call and until requestDone() is called, all db operations should use the same underlying connection.
     * This is useful to ensure that operations happen in a certain order with predictable results.
     */
    public void requestStart() {
        mongo.requestStart();
    }

    /**
     * ends the current "consistent request"
     */
    public void requestDone() {
        mongo.requestDone();
    }

    /**
     * ensure that a connection is assigned to the current "consistent request" (from primary pool, if connected to a replica set)
     */
    public void requestEnsureConnection() {
        requestStart();
    }


    public DBCollection getCollection(String name) {
        DBCollection collection = collectionCache.get(name);
        if (collection != null)
            return collection;

        PrimitiveSerializers primitiveSerializers = PrimitiveSerializers.createDefault();
        collection = new DBCollection(database.getTypedCollection(name, primitiveSerializers, new DBObjectSerializer(primitiveSerializers)), this);
        DBCollection old = collectionCache.putIfAbsent(name, collection);
        return old != null ? old : collection;
    }

}