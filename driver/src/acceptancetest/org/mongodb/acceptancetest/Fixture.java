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

package org.mongodb.acceptancetest;

import org.junit.Assert;
import org.mongodb.MongoClient;
import org.mongodb.MongoClients;
import org.mongodb.ServerAddress;

import java.net.UnknownHostException;

/**
 * Helper class for the acceptance tests.  Considering replacing with MongoClientTestBase.
 */
public final class Fixture {
    private static MongoClient mongoClient;

    private static final String SERVER_NAME = "localhost";
    private static final int PORT = 27017;

    private Fixture() {
    }

    public static synchronized MongoClient createMongoClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(createServerAddress());
        }
        return mongoClient;
    }

    private static ServerAddress createServerAddress() {
        try {
            return new ServerAddress(SERVER_NAME, PORT);
        } catch (UnknownHostException e) {
            Assert.fail("Creating connection to Mongo server failed: " + e.getMessage());
        }
        return null;
    }
}
