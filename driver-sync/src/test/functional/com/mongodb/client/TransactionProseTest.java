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

import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.connection.SocketSettings;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/blob/master/source/transactions/tests/README.rst#mongos-pinning-prose-tests
public class TransactionProseTest {
    private MongoClient client;
    private MongoCollection<Document> collection;

    @Before
    public void setUp() {
        assumeTrue(canRunTests());
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(getMultiMongosConnectionString());

        client = MongoClients.create(MongoClientSettings.builder(builder.build())
                .applyToSocketSettings(new Block<SocketSettings.Builder>() {
                    @Override
                    public void apply(final SocketSettings.Builder builder) {
                        builder.readTimeout(5, TimeUnit.SECONDS);
                    }
                })
                .build());

        collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());
        collection.drop();
    }

    @After
    public void tearDown() {
        if (collection != null) {
            collection.drop();
        }
        if (client != null) {
            client.close();
        }
    }

        // Test that starting a new transaction on a pinned ClientSession unpins the session and normal
    // server selection is performed for the next operation.
    @Test
    public void testNewTransactionUnpinsSession() throws MongoException {
        ClientSession session = null;
        try {
            collection.insertOne(Document.parse("{}"));
            session = client.startSession();
            session.startTransaction();
            collection.insertOne(session, Document.parse("{ _id : 1 }"));
            session.commitTransaction();

            Set<FindIterable<Document>> addresses = new HashSet<>();
            int iterations = 50;
            while (iterations-- > 0) {
                session.startTransaction();
                addresses.add(collection.find(session, Document.parse("{}")));
                session.commitTransaction();
            }
            assertTrue(addresses.size() > 1);
        } finally {
            if (session != null) {
                session.close();
            }
            if (collection != null) {
                collection.drop();
            }
        }
    }

    // Test non-transaction operations using a pinned ClientSession unpins the session and normal server selection is performed.
    @Test
    public void testNonTransactionOpsUnpinsSession() throws MongoException {
        ClientSession session = null;
        try {
            collection.insertOne(Document.parse("{}"));
            session = client.startSession();
            session.startTransaction();
            collection.insertOne(session, Document.parse("{ _id : 1 }"));

            Set<FindIterable<Document>> addresses = new HashSet<>();
            int iterations = 50;
            while (iterations-- > 0) {
                addresses.add(collection.find(session, Document.parse("{}")));
            }
            assertTrue(addresses.size() > 1);
        } finally {
            if (session != null) {
                session.close();
            }
            if (collection != null) {
                collection.drop();
            }
        }
    }

    private boolean canRunTests() {
        if (isSharded()) {
            return serverVersionAtLeast(4, 2);
        } else {
            return false;
        }
    }

}
