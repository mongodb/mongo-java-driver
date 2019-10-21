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
import com.mongodb.MongoNamespace;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.WriteConcern;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
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
                .applyToConnectionPoolSettings(new Block<ConnectionPoolSettings.Builder>() {
                    @Override
                    public void apply(final ConnectionPoolSettings.Builder builder) {
                        builder.addConnectionPoolListener(connectionPoolListener);
                    }
                }).build();

        collectionHelper = new CollectionHelper<Document>(new DocumentCodec(),
                new MongoNamespace(getDefaultDatabaseName(), COLLECTION_NAME));
        client = MongoClients.create(settings);
        MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
        collection = client.getDatabase(getDefaultDatabaseName()).getCollection(COLLECTION_NAME);
        collection.withWriteConcern(WriteConcern.MAJORITY).drop();

        database.withWriteConcern(WriteConcern.MAJORITY).createCollection(COLLECTION_NAME);
    }

    @After
    public void tearDown() {
        if (client != null) {
            collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand', mode: 'off'}");

            try {
                client.getDatabase(getDefaultDatabaseName()).drop();
            } catch (MongoNotPrimaryException e) {
                // GetMore will use the same connection so won't force an server description update
                client.getDatabase(getDefaultDatabaseName()).drop();
            }
            client.close();
        }
    }

    @Test
    public void testGetMoreIteration() {
        assumeTrue(serverVersionAtLeast(asList(4, 1, 10)));

        List<Document> documents = asList(Document.parse("{_id: 1}"), Document.parse("{_id: 2}"), Document.parse("{_id: 3}"),
                Document.parse("{_id: 4}"), Document.parse("{_id: 5}"));
        collection.withWriteConcern(WriteConcern.MAJORITY).insertMany(documents);

        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);
        MongoCursor<Document> cursor = collection.find().batchSize(2).iterator();
        assertEquals(asList(documents.get(0), documents.get(1)), asList(cursor.next(), cursor.next()));

        collectionHelper.runAdminCommand("{replSetStepDown: 5, force: true}");

        assertEquals(asList(documents.get(2), documents.get(3), documents.get(4)), asList(cursor.next(), cursor.next(), cursor.next()));
        assertEquals(connectionCount, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testNotMasterKeepConnectionPool() {
        assumeTrue(serverVersionAtLeast(asList(4, 1, 10)));

        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, data: {failCommands: ['insert'], "
                + "errorCode: 10107}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            collection.insertOne(new Document());
            fail();
        } catch (MongoException e) {
            assertEquals(10107, e.getCode());
        }

        collection.insertOne(new Document());
        assertEquals(connectionCount, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testNotMasterClearConnectionPool() {
        assumeFalse(serverVersionAtLeast(asList(4, 1, 0)));

        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, data: {failCommands: ['insert'], "
                + "errorCode: 10107}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            collection.insertOne(new Document());
            fail();
        } catch (MongoException e) {
            assertEquals(10107, e.getCode());
        }

        collection.insertOne(new Document());
        assertEquals(connectionCount + 1, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testInterruptedAtShutdownResetsConnectionPool() {
        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, data: {failCommands: ['insert'], "
                + "errorCode: 11600}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            collection.insertOne(new Document());
            fail();
        } catch (MongoException e) {
            assertEquals(11600, e.getCode());
        }

        collection.insertOne(new Document());
        assertEquals(connectionCount + 1, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testShutdownInProgressResetsConnectionPool() {
        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, data: {failCommands: ['insert'], "
                + "errorCode: 91}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            collection.insertOne(new Document());
            fail();
        } catch (MongoException e) {
            assertEquals(91, e.getCode());
        }

        collection.insertOne(new Document());
        assertEquals(connectionCount + 1, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

}
