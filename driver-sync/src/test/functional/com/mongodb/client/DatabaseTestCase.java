/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client;

import com.mongodb.client.test.CollectionHelper;
import com.mongodb.internal.connection.ServerHelper;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getPrimary;

public class DatabaseTestCase {
    //For ease of use and readability, in this specific case we'll allow protected variables
    //CHECKSTYLE:OFF
    protected MongoClient client;
    protected MongoDatabase database;
    protected MongoCollection<Document> collection;
    //CHECKSTYLE:ON

    @BeforeEach
    public void setUp() {
        client = getMongoClient();
        database = client.getDatabase(getDefaultDatabaseName());
        collection = database.getCollection(getClass().getName());
        collection.drop();
    }

    @AfterEach
    public void tearDown() {
        if (collection != null) {
            collection.drop();
        }
        try {
            ServerHelper.checkPool(getPrimary());
        } catch (InterruptedException e) {
            // ignore
        }
    }

    protected CollectionHelper<Document> getCollectionHelper() {
        return new CollectionHelper<>(new DocumentCodec(), collection.getNamespace());
    }
}
