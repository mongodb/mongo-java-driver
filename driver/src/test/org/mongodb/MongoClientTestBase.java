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
 */

package org.mongodb;

import org.bson.types.Document;
import org.junit.Before;
import org.mongodb.serialization.CollectibleSerializer;

import java.net.UnknownHostException;

/**
 * Base class for integration-style "unit" tests - i.e. those that need a running Mongo instance.
 */
public abstract class MongoClientTestBase {
    protected static final String DEFAULT_DB_NAME = "driver-test";

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private MongoCollection<Document> collection;

    protected MongoClientTestBase() {
        if (mongoClient == null) {
            try {
                mongoClient = MongoClients.create(new ServerAddress());
                database = mongoClient.getDatabase(DEFAULT_DB_NAME);
                database.admin().drop();
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    @Before
    public void before() {
        collection = getDatabase().getCollection(getClass().getSimpleName());
        collection.admin().drop();
    }

    protected MongoClient getClient() {
        return mongoClient;
    }

    protected MongoDatabase getDatabase() {
        return database;
    }

    protected MongoCollection<Document> getCollection() {
        return collection;
    }

    protected <T> MongoCollection<T> getCollection(final CollectibleSerializer<T> serializer) {
        return database.getTypedCollection(getClass().getSimpleName(), serializer);
    }
}
