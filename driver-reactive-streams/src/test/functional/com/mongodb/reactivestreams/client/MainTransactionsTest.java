/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.AbstractMainTransactionsTest;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.mongodb.reactivestreams.client.syncadapter.ContextHelper.CONTEXT_PROVIDER;
import static com.mongodb.reactivestreams.client.syncadapter.ContextHelper.assertContextPassedThrough;

public class MainTransactionsTest extends AbstractMainTransactionsTest {
    public static final Set<String> SESSION_CLOSE_TIMING_SENSITIVE_TESTS = new HashSet<>(Collections.singletonList(
            "implicit abort"));

    public MainTransactionsTest(final String filename, final String description, final String databaseName, final String collectionName,
                                final BsonArray data, final BsonDocument definition, final boolean skipTest) {
        super(filename, description, databaseName, collectionName, data, definition, skipTest);
    }

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return new SyncMongoClient(MongoClients.create(
                MongoClientSettings.builder(settings).contextProvider(CONTEXT_PROVIDER).build()
        ));
    }

    @Override
    protected StreamFactoryFactory getStreamFactoryFactory() {
        return ClusterFixture.getOverriddenStreamFactoryFactory();
    }

    @Override
    public void shouldPassAllOutcomes() {
        super.shouldPassAllOutcomes();
        assertContextPassedThrough(getDefinition());
    }

    @Before
    public void before() {
        if (SESSION_CLOSE_TIMING_SENSITIVE_TESTS.contains(getDescription())) {
            SyncMongoClient.enableSleepAfterSessionClose(256);
        }
    }

    @After
    public void after() {
        SyncMongoClient.disableSleep();
    }
}
