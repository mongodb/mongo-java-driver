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

import com.mongodb.client.test.CollectionHelper;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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
}
