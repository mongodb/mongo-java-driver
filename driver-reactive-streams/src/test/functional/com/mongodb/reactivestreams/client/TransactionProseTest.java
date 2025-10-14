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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.session.ClientSession;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.ClusterFixture.isSharded;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TransactionProseTest {
    private MongoClient client;
    private MongoCollection<Document> collection;

    @BeforeEach
    public void setUp() {
        assumeTrue(isSharded());
        ConnectionString multiMongosConnectionString = getMultiMongosConnectionString();
        assumeTrue(multiMongosConnectionString != null);

        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(multiMongosConnectionString);

        client = MongoClients.create(MongoClientSettings.builder(builder.build())
                .applyToSocketSettings(builder1 -> builder1.readTimeout(5, TimeUnit.SECONDS))
                .build());

        collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());
        StepVerifier.create(Mono.from(collection.drop())).verifyComplete();
    }

    @AfterEach
    public void tearDown() {
        if (collection != null) {
            StepVerifier.create(Mono.from(collection.drop())).verifyComplete();
        }
        if (client != null) {
            client.close();
        }
    }

    @DisplayName("Mongos Pinning Prose Tests: 1. Test that starting a new transaction on a pinned ClientSession unpins the session "
            + "and normal server selection is performed for the next operation.")
    @Test
    void testNewTransactionUnpinsSession() {
        abort("There is no ability to get the server address with the reactive api");
    }

    @DisplayName("Mongos Pinning Prose Tests: 2. Test non-transaction operations using a pinned ClientSession unpins the session"
            + " and normal server selection is performed")
    @Test
    void testNonTransactionOpsUnpinsSession() {
        abort("There is no ability to get the server address with the reactive api");
    }

    @DisplayName("Options Inside Transaction Prose Tests. 1. Write concern not inherited from collection object inside transaction")
    @Test
    void testWriteConcernInheritance() {
        Mono<Document> testWriteConcern = Mono.from(client.startSession())
                .flatMap(clientSession ->
                        Mono.using(() -> clientSession,
                                session -> Mono.fromRunnable(session::startTransaction)
                                        .then(Mono.from(collection.withWriteConcern(new WriteConcern(0)).insertOne(session, new Document("n", 1))))
                                        .flatMap( r -> Mono.from(session.commitTransaction()))
                                        .then(Mono.from(collection.find(new Document("n", 1)).first())),
                                ClientSession::close
                        )
                );

        StepVerifier.create(testWriteConcern).assertNext(Assertions::assertNotNull).verifyComplete();
    }
}
