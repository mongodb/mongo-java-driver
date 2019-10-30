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

package com.mongodb.internal.async.client;

import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.WriteConcern;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.internal.async.client.Fixture.getMongoClientSettings;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
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
    public void setUp() throws InterruptedException {
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

        FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
        collection.withWriteConcern(WriteConcern.MAJORITY).drop(callback);
        callback.get(30, TimeUnit.SECONDS);

        callback = new FutureResultCallback<Void>();
        database.withWriteConcern(WriteConcern.MAJORITY).createCollection(COLLECTION_NAME, callback);
        callback.get(30, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() throws InterruptedException {
        if (client != null) {
            collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand', mode: 'off'}");

            try {
                FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
                client.getDatabase(getDefaultDatabaseName()).drop(callback);
                callback.get(30, TimeUnit.SECONDS);
            } catch (MongoNotPrimaryException e) {
                // GetMore will use the same connection so won't force an server description update
                FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
                client.getDatabase(getDefaultDatabaseName()).drop(callback);
                callback.get(30, TimeUnit.SECONDS);
            }
            client.close();
        }
    }

    @Test
    public void testGetMoreIteration() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(asList(4, 1, 10)));

        List<Document> documents = asList(Document.parse("{_id: 1}"), Document.parse("{_id: 2}"), Document.parse("{_id: 3}"),
                Document.parse("{_id: 4}"), Document.parse("{_id: 5}"));
        FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
        collection.withWriteConcern(WriteConcern.MAJORITY).insertMany(documents, callback);
        callback.get(30, TimeUnit.SECONDS);

        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        FutureResultCallback<AsyncBatchCursor<Document>> batchCursorCallback = new FutureResultCallback<AsyncBatchCursor<Document>>();
        collection.find().batchSize(2).batchCursor(batchCursorCallback);
        AsyncBatchCursor<Document> cursor = batchCursorCallback.get(30, TimeUnit.SECONDS);

        FutureResultCallback<List<Document>> batchCallback = new FutureResultCallback<List<Document>>();
        cursor.next(batchCallback);
        assertEquals(asList(documents.get(0), documents.get(1)), batchCallback.get());

        collectionHelper.runAdminCommand("{replSetStepDown: 5, force: true}");

        batchCallback = new FutureResultCallback<List<Document>>();
        cursor.next(batchCallback);
        assertEquals(asList(documents.get(2), documents.get(3)), batchCallback.get(30, TimeUnit.SECONDS));

        batchCallback = new FutureResultCallback<List<Document>>();
        cursor.next(batchCallback);
        assertEquals(singletonList(documents.get(4)), batchCallback.get(30, TimeUnit.SECONDS));
        assertEquals(connectionCount, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testNotMasterKeepConnectionPool() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(asList(4, 1, 10)));

        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, data: {failCommands: ['insert'], "
                + "errorCode: 10107}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
            collection.insertOne(new Document(), callback);
            callback.get(30, TimeUnit.SECONDS);
            fail();
        } catch (MongoException e) {
            assertEquals(10107, e.getCode());
        }

        FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
        collection.insertOne(new Document(), callback);
        callback.get(30, TimeUnit.SECONDS);
        assertEquals(connectionCount, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testNotMasterClearConnectionPool() throws InterruptedException {
        assumeFalse(serverVersionAtLeast(asList(4, 1, 0)));

        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, data: {failCommands: ['insert'], "
                + "errorCode: 10107}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
            collection.insertOne(new Document(), callback);
            callback.get(30, TimeUnit.SECONDS);
            fail();
        } catch (MongoException e) {
            assertEquals(10107, e.getCode());
        }
        assertEquals(1, connectionPoolListener.countEvents(ConnectionPoolClearedEvent.class));

        FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
        collection.insertOne(new Document("test", 1), callback);
        callback.get(30, TimeUnit.SECONDS);
        assertEquals(connectionCount + 1, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testInterruptedAtShutdownResetsConnectionPool() throws InterruptedException {
        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, data: {failCommands: ['insert'], "
                + "errorCode: 11600}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
            collection.insertOne(new Document(), callback);
            callback.get(30, TimeUnit.SECONDS);
        } catch (MongoException e) {
            assertEquals(11600, e.getCode());
        }
        assertEquals(1, connectionPoolListener.countEvents(ConnectionPoolClearedEvent.class));

        FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
        collection.insertOne(new Document("test", 1), callback);
        callback.get(30, TimeUnit.SECONDS);
        assertEquals(connectionCount + 1, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

    @Test
    public void testShutdownInProgressResetsConnectionPool() throws InterruptedException {
        collectionHelper.runAdminCommand("{configureFailPoint: 'failCommand',  mode: {times: 1}, data: {failCommands: ['insert'], "
                + "errorCode: 91}}");
        int connectionCount = connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class);

        try {
            FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
            collection.insertOne(new Document(), callback);
            callback.get(30, TimeUnit.SECONDS);
        } catch (MongoException e) {
            assertEquals(91, e.getCode());
        }
        assertEquals(1, connectionPoolListener.countEvents(ConnectionPoolClearedEvent.class));

        FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
        collection.insertOne(new Document("test", 1), callback);
        callback.get(30, TimeUnit.SECONDS);
        assertEquals(connectionCount + 1, connectionPoolListener.countEvents(com.mongodb.event.ConnectionAddedEvent.class));
    }

}
