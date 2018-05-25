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

import com.mongodb.MongoClientException;
import com.mongodb.client.MongoClient;
import org.junit.Ignore;
import org.junit.Test;

import static com.mongodb.embedded.client.Fixture.getMongoClientSettings;
import static com.mongodb.embedded.client.Fixture.getMongoEmbeddedSettings;
import static org.junit.Assert.fail;

@Ignore  // TODO - Enable again in the future once more init & fini can be called multiple times in a process.
public class MongoClientsTest extends DatabaseTestCase {

    @Ignore
    @Test(expected = MongoClientException.class)
    public void shouldThrowIfInitCalledTwice() {
        MongoClients.init(getMongoEmbeddedSettings());
        MongoClients.init(getMongoEmbeddedSettings());
    }

    @Test(expected = MongoClientException.class)
    public void shouldThrowIfCreateCalledWithoutCallingInit() {
        MongoClients.create(getMongoClientSettings());
    }

    @Test(expected = MongoClientException.class)
    public void shouldThrowIfUsingMongoClientAfterClose() {
        MongoClient mongoClient = MongoClients.create(getMongoClientSettings());
        try {
            MongoClients.close();
        } catch (MongoClientException e) {
            mongoClient.close();
            throw e;
        }
        fail();
    }

}
