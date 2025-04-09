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

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.getServerStatus;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#prose-tests">Retryable Write Prose Tests</a>.
 */
public class RetryableWritesProseTest extends DatabaseTestCase {
    private CollectionHelper<Document> collectionHelper;

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();

        collectionHelper = new CollectionHelper<>(new DocumentCodec(), collection.getNamespace());
        collectionHelper.create();
    }

    @Test
    public void testRetryWritesWithInsertOneAgainstMMAPv1RaisesError() {
        assumeTrue(canRunMmapv1Tests());
        boolean exceptionFound = false;

        try {
            Mono.from(collection.insertOne(Document.parse("{ x : 1 }"))).block(TIMEOUT_DURATION);
        } catch (MongoClientException e) {
            assertEquals("This MongoDB deployment does not support retryable writes. "
                    + "Please add retryWrites=false to your connection string.", e.getMessage());
            assertEquals(20, ((MongoException) e.getCause()).getCode());
            assertTrue(e.getCause().getMessage().contains("Transaction numbers"));
            exceptionFound = true;
        }
        assertTrue(exceptionFound);
    }

    @Test
    public void testRetryWritesWithFindOneAndDeleteAgainstMMAPv1RaisesError() {
        assumeTrue(canRunMmapv1Tests());
        boolean exceptionFound = false;

        try {
            Mono.from(collection.findOneAndDelete(Document.parse("{ x : 1 }"))).block(TIMEOUT_DURATION);
        } catch (MongoClientException e) {
            assertEquals("This MongoDB deployment does not support retryable writes. "
                    + "Please add retryWrites=false to your connection string.", e.getMessage());
            assertEquals(20, ((MongoException) e.getCause()).getCode());
            assertTrue(e.getCause().getMessage().contains("Transaction numbers"));
            exceptionFound = true;
        }
        assertTrue(exceptionFound);
    }

    /**
     * Prose test #2.
     */
    @Test
    public void poolClearedExceptionMustBeRetryable() throws InterruptedException, ExecutionException, TimeoutException {
        com.mongodb.client.RetryableWritesProseTest.poolClearedExceptionMustBeRetryable(
                mongoClientSettings -> new SyncMongoClient(MongoClients.create(mongoClientSettings)),
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    /**
     * Prose test #3.
     */
    @Test
    public void originalErrorMustBePropagatedIfNoWritesPerformed() throws InterruptedException {
        com.mongodb.client.RetryableWritesProseTest.originalErrorMustBePropagatedIfNoWritesPerformed(
                mongoClientSettings -> new SyncMongoClient(MongoClients.create(mongoClientSettings)));
    }

    /**
     * Prose test #4.
     */
    @Test
    public void retriesOnDifferentMongosWhenAvailable() {
        com.mongodb.client.RetryableWritesProseTest.retriesOnDifferentMongosWhenAvailable(
                mongoClientSettings -> new SyncMongoClient(MongoClients.create(mongoClientSettings)),
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    /**
     * Prose test #5.
     */
    @Test
    public void retriesOnSameMongosWhenAnotherNotAvailable() {
        com.mongodb.client.RetryableWritesProseTest.retriesOnSameMongosWhenAnotherNotAvailable(
                mongoClientSettings -> new SyncMongoClient(MongoClients.create(mongoClientSettings)),
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    private boolean canRunMmapv1Tests() {
        Document storageEngine = (Document) getServerStatus().get("storageEngine");

        return ((isSharded() || isDiscoverableReplicaSet())
                && storageEngine != null && storageEngine.get("name").equals("mmapv1")
                && serverVersionLessThan(4, 2));
    }
}
