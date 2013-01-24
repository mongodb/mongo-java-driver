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
    public static final String DEFAULT_DB_NAME = "driver-test";
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private MongoCollection<Document> collection;

    protected MongoClientTestBase() {
        synchronized (MongoClientTestBase.class) {
            if (mongoClient == null) {
                String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
                final String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty()
                        ? DEFAULT_URI : mongoURIProperty;
                System.out.println("URI: " + mongoURIString);
                try {
                    mongoClient = MongoClients.create(new MongoClientURI(mongoURIString));
                    database = mongoClient.getDatabase(DEFAULT_DB_NAME);
                    database.admin().drop();
                } catch (UnknownHostException e) {
                    throw new IllegalArgumentException("Invalid Mongo URI: " + mongoURIString, e);
                }
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
        return database.getCollection(getClass().getSimpleName(), serializer);
    }
}
