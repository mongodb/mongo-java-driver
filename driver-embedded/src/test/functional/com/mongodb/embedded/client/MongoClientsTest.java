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

package com.mongodb.embedded.client;

import com.mongodb.client.MongoClient;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.mongodb.embedded.client.Fixture.getMongoClientSettings;
import static com.mongodb.embedded.client.Fixture.getMongoEmbeddedSettings;
import static org.junit.Assert.fail;

public class MongoClientsTest {

    @BeforeClass
    public static void beforeAll() {
        Fixture.close();
    }

    @Test(expected = MongoClientEmbeddedException.class)
    public void shouldThrowIfInitCalledTwice() {
        try {
            MongoClients.init(getMongoEmbeddedSettings());
            MongoClients.init(getMongoEmbeddedSettings());
            fail();
        } finally {
            MongoClients.close();
        }
    }

    @Test(expected = MongoClientEmbeddedException.class)
    public void shouldThrowIfCreateCalledWithoutCallingInit() {
        MongoClients.create(getMongoClientSettings());
    }

    @Test(expected = MongoClientEmbeddedException.class)
    public void shouldThrowIfLibHandlesStillExist() {
        MongoClients.init(getMongoEmbeddedSettings());
        MongoClient mongoClient = MongoClients.create(getMongoClientSettings());
        try {
            MongoClients.close();
            fail();
        } finally {
            mongoClient.close();
            MongoClients.close();
        }
    }

    @Test(expected = MongoClientEmbeddedException.class)
    public void shouldThrowWhenTryingToOpenMultipleClients() {
        MongoClients.init(getMongoEmbeddedSettings());
        MongoClient mongoClient = MongoClients.create(getMongoClientSettings());
        try {
            MongoClients.create(getMongoClientSettings());
            fail();
        } finally {
            mongoClient.close();
            MongoClients.close();
        }
    }
}
