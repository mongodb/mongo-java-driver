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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoNamespace;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.ServerHelper;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.sleep;
import static com.mongodb.client.Fixture.getPrimary;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientBuilderFromConnectionString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * This is the Java alternative to {@link MongoClientSessionSpecification}.
 * New tests should be added here instead of the Groovy specification.
 * Tests from the Groovy specification should be gradually migrated to this class.
 * </p>
 */
public class MongoClientSessionFunctionalTest {
    private static final MongoNamespace NAMESPACE =
            new MongoNamespace(getDefaultDatabaseName(),
                    MongoClientSessionFunctionalTest.class.getSimpleName());
    private static final String FAIL_COMMAND_NAME = "failCommand";
    private CollectionHelper<BsonDocument> collectionHelper;

    @BeforeEach
    public void setUp() {
        collectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), NAMESPACE);
        collectionHelper.create();
    }

    @Test
    @DisplayName("should issue only one abortTransaction when close is called multiple times")
    void shouldIssueOnlyOneAbortTransactionWhenCloseCalledMultipleTimes() {
        assumeTrue(isDiscoverableReplicaSet());

        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: '" + FAIL_COMMAND_NAME + "',"
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: ['abortTransaction'],"
                + "    blockConnection: true,"
                + "    blockTimeMS: 50"
                + "  }"
                + "}");

        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = MongoClients.create(getMongoClientBuilderFromConnectionString()
                .addCommandListener(commandListener)
                .build())) {

            MongoCollection<Document> collection = client.getDatabase(NAMESPACE.getDatabaseName())
                    .getCollection(NAMESPACE.getCollectionName());

            ClientSession session = Mono.from(client.startSession()).block(TIMEOUT_DURATION);
            session.startTransaction();
            Mono.from(collection.insertOne(session, new Document("x", 1))).block(TIMEOUT_DURATION);

            // when
            commandListener.reset();
            session.close();
            session.close();
            session.close();

            // wait for async abort operations to complete
            sleep(1000);

            // then
            List<CommandStartedEvent> abortCommands = commandListener.getCommandStartedEvents().stream()
                    .filter(event -> event.getCommandName().equals("abortTransaction"))
                    .collect(Collectors.toList());

            assertEquals(1, abortCommands.size(), "Expected exactly one abortTransaction command but was : " + abortCommands.size());
        }
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        ClusterFixture.disableFailPoint(FAIL_COMMAND_NAME);
        if (collectionHelper != null) {
            // Due to testing abortTransaction via failpoint, there may be open transactions
            // after the test finishes, thus drop() command hangs for 60 seconds until transaction
            // is automatically rolled back.
            collectionHelper.runAdminCommand("{killAllSessions: []}");
            collectionHelper.drop();
            try {
                ServerHelper.checkPool(getPrimary());
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}
