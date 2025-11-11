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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// See https://github.com/mongodb/specifications/blob/master/source/transactions/tests/README.md#mongos-pinning-prose-tests
public class TransactionProseTest {
    private MongoClient client;
    private MongoCollection<Document> collection;

    @BeforeEach
    public void setUp() {
        assumeTrue(!isStandalone());
        ConnectionString connectionString = getConnectionString();
        if (isSharded()) {
            assumeTrue(serverVersionAtLeast(4, 2));
            connectionString = getMultiMongosConnectionString();
            assumeTrue(connectionString != null);
        }

        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build());

        collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());
        collection.drop();
    }

    @AfterEach
    public void tearDown() {
        if (collection != null) {
            collection.drop();
        }
        if (client != null) {
            client.close();
        }
    }

    @DisplayName("Mongos Pinning Prose Tests: 1. Test that starting a new transaction on a pinned ClientSession unpins the session "
            + "and normal server selection is performed for the next operation.")
    @Test
    public void testNewTransactionUnpinsSession() {
        assumeTrue(isSharded());
        collection.insertOne(Document.parse("{}"));
        try (ClientSession session = client.startSession()) {
            session.startTransaction();
            collection.insertOne(session, Document.parse("{ _id : 1 }"));
            session.commitTransaction();

            Set<ServerAddress> addresses = new HashSet<>();
            int iterations = 50;
            while (iterations-- > 0) {
                session.startTransaction();
                try (MongoCursor<Document> cursor = collection.find(session, Document.parse("{}")).cursor()) {
                    addresses.add(cursor.getServerAddress());
                }
                session.commitTransaction();
            }
            assertTrue(addresses.size() > 1);
        }
    }

    @DisplayName("Mongos Pinning Prose Tests: 2. Test non-transaction operations using a pinned ClientSession unpins the session"
            + " and normal server selection is performed")
    @Test
    public void testNonTransactionOpsUnpinsSession() throws MongoException {
        assumeTrue(isSharded());
        collection.insertOne(Document.parse("{}"));
        try (ClientSession session = client.startSession()) {
            session.startTransaction();
            collection.insertOne(session, Document.parse("{ _id : 1 }"));
            session.commitTransaction();

            Set<ServerAddress> addresses = new HashSet<>();
            int iterations = 50;
            while (iterations-- > 0) {
                try (MongoCursor<Document> cursor = collection.find(session, Document.parse("{}")).cursor()) {
                    addresses.add(cursor.getServerAddress());
                }
            }
            assertTrue(addresses.size() > 1);
        }
    }

    @DisplayName("Options Inside Transaction Prose Tests. 1. Write concern not inherited from collection object inside transaction")
    @Test
    void testWriteConcernInheritance() {
        // Create a MongoClient running against a configured sharded/replica set/load balanced cluster.
        // transactions require a 4.0+ server when non-sharded and 4.2+ when sharded
        if (isSharded()) {
            assumeTrue(serverVersionAtLeast(4, 2));
        } else {
            assumeTrue(serverVersionAtLeast(4, 0));
        }

        collection.insertOne(Document.parse("{}"));

        try (ClientSession session = client.startSession()) {
            MongoCollection<Document> wcCollection = collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED);

            assertDoesNotThrow(() -> {
                        session.startTransaction();
                        wcCollection.insertOne(session, new Document("n", 1));
                        session.commitTransaction();
                    });
            assertNotNull(collection.find(new Document("n", 1)).first());
        }
    }

}
