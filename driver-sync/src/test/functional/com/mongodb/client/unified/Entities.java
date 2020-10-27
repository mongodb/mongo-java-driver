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

package com.mongodb.client.unified;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.Assume.assumeTrue;

final class Entities {
    private final Map<String, MongoClient> clients = new HashMap<>();
    private final Map<String, MongoDatabase> databases = new HashMap<>();
    private final Map<String, MongoCollection<BsonDocument>> collections = new HashMap<>();
    private final Map<String, ClientSession> sessions = new HashMap<>();
    private final Map<String, BsonDocument> sessionIdentifiers = new HashMap<>();
    private final Map<String, GridFSBucket> buckets = new HashMap<>();
    private final Map<String, TestCommandListener> clientCommandListeners = new HashMap<>();
    private final Map<String, MongoCursor<BsonDocument>> changeStreams = new HashMap<>();

    public void addChangeStream(final String id, final MongoCursor<BsonDocument> cursor) {
        changeStreams.put(id, cursor);
    }

    public MongoCursor<BsonDocument> getChangeStream(final String id) {
        MongoCursor<BsonDocument> changeStream = changeStreams.get(id);
        if (changeStream == null) {
            throw new IllegalStateException("Missing change stream with id: " + id);
        }
        return changeStream;
    }

    public boolean hasClient(final String id) {
        return clients.containsKey(id);
    }

    public MongoClient getClient(final String id) {
        MongoClient mongoClient = clients.get(id);
        if (mongoClient == null) {
            throw new IllegalStateException("Missing client with id: " + id);
        }
        return mongoClient;
    }

    public boolean hasDatabase(final String id) {
        return databases.containsKey(id);
    }

    public MongoDatabase getDatabase(final String id) {
        MongoDatabase database = databases.get(id);
        if (database == null) {
            throw new IllegalStateException("Missing database with id: " + id);
        }
        return database;
    }

    public boolean hasCollection(final String id) {
        return collections.containsKey(id);
    }

    public MongoCollection<BsonDocument> getCollection(final String id) {
        MongoCollection<BsonDocument> collection = collections.get(id);
        if (collection == null) {
            throw new IllegalStateException("Missing collection with id: " + id);
        }
        return collection;
    }

    public ClientSession getSession(final String id) {
        ClientSession clientSession = sessions.get(id);
        if (clientSession == null) {
            throw new IllegalStateException("Missing session with id: " + id);
        }
        return clientSession;
    }

    public BsonDocument getSessionIdentifier(final String id) {
        BsonDocument sessionIdentifier = sessionIdentifiers.get(id);
        if (sessionIdentifier == null) {
            throw new IllegalStateException("Missing session identifier with id: " + id);
        }
        return sessionIdentifier;
    }

    public GridFSBucket getBucket(final String id) {
        GridFSBucket bucket = buckets.get(id);
        if (bucket == null) {
            throw new IllegalStateException("Missing bucket with id: " + id);
        }
        return bucket;
    }

    public TestCommandListener getClientCommandListener(final String id) {
        TestCommandListener commandListener = clientCommandListeners.get(id);
        if (commandListener == null) {
            throw new IllegalStateException("Missing command listener with id: " + id);
        }
        return commandListener;
    }

    public void init(final BsonArray entitiesArray, final MongoClientSupplier mongoClientSupplier) {
        for (BsonValue cur : entitiesArray.getValues()) {
            String entityType = cur.asDocument().getFirstKey();
            BsonDocument entity = cur.asDocument().getDocument(entityType);
            String id = entity.getString("id").getValue();
            switch (entityType) {
                case "client":
                    MongoClientSettings.Builder clientSettingsBuilder = getMongoClientSettingsBuilder();
                    if (entity.getBoolean("useMultipleMongoses", BsonBoolean.FALSE).getValue()) {
                        assumeTrue(getMultiMongosConnectionString() != null);
                        clientSettingsBuilder.applyConnectionString(getMultiMongosConnectionString());
                    }
                    if (entity.containsKey("observeEvents")) {
                        List<String> ignoreCommandMonitoringEvents = entity
                                .getArray("ignoreCommandMonitoringEvents", new BsonArray()).stream()
                                .map(type -> type.asString().getValue()).collect(Collectors.toList());
                        ignoreCommandMonitoringEvents.add("configureFailPoint");
                        TestCommandListener testCommandListener = new TestCommandListener(
                                entity.getArray("observeEvents").stream()
                                        .map(type -> type.asString().getValue()).collect(Collectors.toList()),
                                ignoreCommandMonitoringEvents);
                        clientSettingsBuilder.addCommandListener(testCommandListener);
                        clientCommandListeners.put(id, testCommandListener);
                    }
                    if (entity.containsKey("uriOptions")) {
                        entity.getDocument("uriOptions").forEach((key, value) -> {
                            switch (key) {
                                case "retryReads":
                                   clientSettingsBuilder.retryReads(value.asBoolean().getValue());
                                   break;
                                case "retryWrites":
                                    clientSettingsBuilder.retryWrites(value.asBoolean().getValue());
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unsupported uri option: " + key);
                            }
                        });
                    }
                    clientSettingsBuilder.applyToServerSettings(builder -> {
                        // TODO: only set if not in uriOptions
                        builder.heartbeatFrequency(50, TimeUnit.MILLISECONDS);
                        builder.minHeartbeatFrequency(50, TimeUnit.MILLISECONDS);
                    });
                    clients.put(id, mongoClientSupplier.get(clientSettingsBuilder.build()));
                    break;
                case "database": {
                    MongoClient client = clients.get(entity.getString("client").getValue());
                    MongoDatabase database = client.getDatabase(entity.getString("databaseName").getValue());
                    if (entity.containsKey("collectionOptions")) {
                        for (Map.Entry<String, BsonValue> entry : entity.getDocument("collectionOptions").entrySet()) {
                            //noinspection SwitchStatementWithTooFewBranches
                            switch (entry.getKey()) {
                                case "readConcern":
                                    database = database.withReadConcern(asReadConcern(entry.getValue()));
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unsupported database option: " + entry.getKey());
                            }
                        }
                    }
                    databases.put(id, database);
                    break;
                }
                case "collection": {
                    MongoDatabase database = databases.get(entity.getString("database").getValue());
                    MongoCollection<BsonDocument> collection = database.getCollection(entity.getString("collectionName").getValue(),
                            BsonDocument.class);
                    if (entity.containsKey("collectionOptions")) {
                        for (Map.Entry<String, BsonValue> entry : entity.getDocument("collectionOptions").entrySet()) {
                            //noinspection SwitchStatementWithTooFewBranches
                            switch (entry.getKey()) {
                                case "readConcern":
                                    collection = collection.withReadConcern(asReadConcern(entry.getValue()));
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unsupported collection option: " + entry.getKey());
                            }
                        }
                    }
                    collections.put(id, collection);
                    break;
                }
                case "session": {
                    MongoClient client = clients.get(entity.getString("client").getValue());
                    ClientSessionOptions options = ClientSessionOptions.builder().build();
                    ClientSession session = client.startSession(options);
                    if (entity.containsKey("sessionOptions")) {
                        throw new UnsupportedOperationException("Unsupported session specification: sessionOptions");
                    }
                    sessions.put(id, session);
                    sessionIdentifiers.put(id, session.getServerSession().getIdentifier());
                    break;
                }
                case "bucket": {
                    MongoDatabase database = databases.get(entity.getString("database").getValue());
                    if (entity.containsKey("bucketOptions")) {
                        throw new UnsupportedOperationException("Unsupported session specification: bucketOptions");
                    }
                    buckets.put(id, GridFSBuckets.create(database));
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unsupported entity type: " + entity.getFirstKey());
            }
        }
    }

    private ReadConcern asReadConcern(final BsonValue value) {
        return new ReadConcern(ReadConcernLevel.fromString(value.asDocument().getString("level").getValue()));
    }

    public void close() {
        for (MongoClient client : clients.values()) {
            client.close();
        }
    }
}
