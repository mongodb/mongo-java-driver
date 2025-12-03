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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The {@link ChangeStreamProseTest}, which is defined only for sync driver, should be migrated to this class.
 * Once this done, this class should be renamed to ChangeStreamProseTest.
 */
public abstract class AbstractChangeSteamFunctionalTest {

    private static final String FAIL_COMMAND_NAME = "failCommand";
    private static final MongoNamespace NAMESPACE = new MongoNamespace(getDefaultDatabaseName(), "test");
    private final CollectionHelper<BsonDocument> collectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), NAMESPACE);

    protected abstract MongoClient createMongoClient(MongoClientSettings mongoClientSettings);

    @Test
    public void shouldDoOneServerSelectionForResumeAttempt() {
        //given
        assumeTrue(ClusterFixture.isDiscoverableReplicaSet());
        AtomicInteger serverSelectionCounter = new AtomicInteger();
        BsonTimestamp startTime = new BsonTimestamp((int) Instant.now().getEpochSecond(), 0);
        try (MongoClient mongoClient = createMongoClient(Fixture.getMongoClientSettingsBuilder()
                .applyToClusterSettings(builder -> builder.serverSelector(clusterDescription -> {
                    serverSelectionCounter.incrementAndGet();
                    return clusterDescription.getServerDescriptions();
                })).build())) {

            MongoCollection<Document> collection = mongoClient
                    .getDatabase(NAMESPACE.getDatabaseName())
                    .getCollection(NAMESPACE.getCollectionName());

            collectionHelper.runAdminCommand("{"
                    + "    configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                    + "    mode: {"
                    + "        times: 1"
                    + "    },"
                    + "    data: {"
                    + "        failCommands: ['getMore'],"
                    + "        errorCode: 10107,"
                    + "        errorLabels: ['ResumableChangeStreamError']"
                    + "    }"
                    + "}");
            // We insert document here, because async cursor performs aggregate and getMore right after we call cursor()
            collection.insertOne(Document.parse("{ x: 1 }"));
            serverSelectionCounter.set(0);

            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch()
                    .batchSize(0)
                    .startAtOperationTime(startTime)
                    .cursor()) {

                //when
                ChangeStreamDocument<Document> changeStreamDocument = cursor.next();
                //then
                assertNotNull(changeStreamDocument);
                int actualCountOfServerSelections = serverSelectionCounter.get();
                assertEquals(2, actualCountOfServerSelections,
                        format("Expected 2 server selections (initial aggregate command  + resume attempt aggregate command), but there were %s",
                                actualCountOfServerSelections));
            }
        }
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        ClusterFixture.disableFailPoint(FAIL_COMMAND_NAME);
        collectionHelper.drop();
    }
}
