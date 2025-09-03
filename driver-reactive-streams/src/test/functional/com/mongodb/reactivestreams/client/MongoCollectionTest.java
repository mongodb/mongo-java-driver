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

package com.mongodb.reactivestreams.client;

import com.mongodb.client.AbstractMongoCollectionTest;
import com.mongodb.client.MongoDatabase;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.junit.jupiter.api.AfterAll;

import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;

public class MongoCollectionTest extends AbstractMongoCollectionTest {

    private static com.mongodb.client.MongoClient mongoClient;

    @Override
    protected MongoDatabase getDatabase(final String databaseName) {
        return createMongoClient().getDatabase(databaseName);
    }

    private com.mongodb.client.MongoClient createMongoClient() {
        if (mongoClient == null) {
            mongoClient = new SyncMongoClient(MongoClients.create(getMongoClientSettingsBuilder().build()));
        }
        return mongoClient;
    }


    @AfterAll
    public static void closeClient() {
        if (mongoClient != null)  {
            mongoClient.close();
            mongoClient = null;
        }
    }
}
