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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.WriteConcern;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientSettings;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/connections-survive-step-down/tests
@SuppressWarnings("deprecation")
public class ConnectionsSurvivePrimaryStepDownProseTest {
    private static final String COLLECTION_NAME = "step-down";

    private TestConnectionPoolListener connectionPoolListener;
    private CollectionHelper<Document> collectionHelper;
    private MongoClient client;
    private MongoCollection<Document> collection;

    @Before
    public void setUp() {
        assumeTrue(isDiscoverableReplicaSet() && serverVersionAtLeast(4, 0));
        connectionPoolListener = new TestConnectionPoolListener();
        MongoClientSettings settings = MongoClientSettings.builder(getMongoClientSettings()).retryWrites(false)
                .applyToConnectionPoolSettings(builder -> builder.addConnectionPoolListener(connectionPoolListener)).build();

        collectionHelper = new CollectionHelper<>(new DocumentCodec(),
                                                  new MongoNamespace(getDefaultDatabaseName(), COLLECTION_NAME));
        client = MongoClients.create(settings);
        MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
        collection = client.getDatabase(getDefaultDatabaseName()).getCollection(COLLECTION_NAME);

        Mono.from(collection.withWriteConcern(WriteConcern.MAJORITY).drop()).block(TIMEOUT_DURATION);
        Mono.from(database.withWriteConcern(WriteConcern.MAJORITY).createCollection(COLLECTION_NAME)).block(TIMEOUT_DURATION);
    }

    @After
    public void tearDown() {
        if (client != null) {
            collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand', mode: 'off'}");
            try {
                Mono.from(client.getDatabase(getDefaultDatabaseName()).drop()).block(TIMEOUT_DURATION);
            } catch (MongoNotPrimaryException e) {
                // GetMore will use the same connection so won't force an server description update
                Mono.from(client.getDatabase(getDefaultDatabaseName()).drop()).block(TIMEOUT_DURATION);
            }
            client.close();
        }
    }

    @Test
    public void testGetMoreIteration() {
        assumeTrue(serverVersionAtLeast(asList(4, 1, 10)));

        List<Document> documents = asList(Document.parse("{_id: 1}"), Document.parse("{_id: 2}"), Document.parse("{_id: 3}"),
                                          Document.parse("{_id: 4}"), Document.parse("{_id: 5}"));
        Mono.from(collection.withWriteConcern(WriteConcern.MAJORITY).insertMany(documents)).block(TIMEOUT_DURATION);

        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        BatchCursor<Document> cursor = ((FindPublisherImpl<Document>) collection.find().batchSize(2)).batchCursor(2)
                .block(TIMEOUT_DURATION);
        assertNotNull(cursor);
        assertEquals(asList(documents.get(0), documents.get(1)), Mono.from(cursor.next()).block(TIMEOUT_DURATION));

        collectionHelper.runAdminCommand("{replSetStepDown: 5, force: true}");

        assertEquals(asList(documents.get(2), documents.get(3)), Mono.from(cursor.next()).block(TIMEOUT_DURATION));
        assertEquals(singletonList(documents.get(4)), Mono.from(cursor.next()).block(TIMEOUT_DURATION));
        assertEquals(connectionCount, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testNotPrimaryKeepConnectionPool() {
        assumeTrue(serverVersionAtLeast(asList(4, 1, 10)));

        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, "
                                                 + "data: {failCommands: ['insert'], errorCode: 10107}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            Mono.from(collection.insertOne(new Document())).block(TIMEOUT_DURATION);
            fail();
        } catch (MongoException e) {
            assertEquals(10107, e.getCode());
        }

        Mono.from(collection.insertOne(new Document())).block(TIMEOUT_DURATION);
        assertEquals(connectionCount, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testNotPrimaryClearConnectionPool() {
        assumeFalse(serverVersionAtLeast(asList(4, 1, 0)));

        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, "
                                                 + "data: {failCommands: ['insert'], errorCode: 10107}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            Mono.from(collection.insertOne(new Document())).block(TIMEOUT_DURATION);
            fail();
        } catch (MongoException e) {
            assertEquals(10107, e.getCode());
        }
        assertEquals(1, connectionPoolListener.countEvents(ConnectionPoolClearedEvent.class));

        Mono.from(collection.insertOne(new Document())).block(TIMEOUT_DURATION);
        assertEquals(connectionCount + 1, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testInterruptedAtShutdownResetsConnectionPool() {
        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, "
                                                 + "data: {failCommands: ['insert'], errorCode: 11600}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            Mono.from(collection.insertOne(new Document())).block(TIMEOUT_DURATION);
        } catch (MongoException e) {
            assertEquals(11600, e.getCode());
        }
        assertEquals(1, connectionPoolListener.countEvents(ConnectionPoolClearedEvent.class));
        Mono.from(collection.insertOne(new Document())).block(TIMEOUT_DURATION);
        assertEquals(connectionCount + 1, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testShutdownInProgressResetsConnectionPool() {
        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, "
                                                 + "data: {failCommands: ['insert'], errorCode: 91}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            Mono.from(collection.insertOne(new Document())).block(TIMEOUT_DURATION);
        } catch (MongoException e) {
            assertEquals(91, e.getCode());
        }
        assertEquals(1, connectionPoolListener.countEvents(ConnectionPoolClearedEvent.class));

        Mono.from(collection.insertOne(new Document())).block(TIMEOUT_DURATION);
        assertEquals(connectionCount + 1, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

}
