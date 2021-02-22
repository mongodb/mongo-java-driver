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
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.unified.UnifiedCrudHelper.asReadConcern;
import static com.mongodb.client.unified.UnifiedCrudHelper.asWriteConcern;
import static java.util.Objects.requireNonNull;
import static org.junit.Assume.assumeTrue;

final class Entities {
    private final Map<String, BsonValue> results = new HashMap<>();
    private final Map<String, MongoClient> clients = new HashMap<>();
    private final Map<String, MongoDatabase> databases = new HashMap<>();
    private final Map<String, MongoCollection<BsonDocument>> collections = new HashMap<>();
    private final Map<String, ClientSession> sessions = new HashMap<>();
    private final Map<String, BsonDocument> sessionIdentifiers = new HashMap<>();
    private final Map<String, GridFSBucket> buckets = new HashMap<>();
    private final Map<String, TestCommandListener> clientCommandListeners = new HashMap<>();
    private final Map<String, MongoCursor<ChangeStreamDocument<BsonDocument>>> changeStreams = new HashMap<>();

    public void addResult(final String id, final BsonValue result) {
        results.put(id, result);
    }

    public BsonValue getResult(final String id) {
        return getEntity(id, results, "result");
    }

    public void addChangeStream(final String id, final MongoCursor<ChangeStreamDocument<BsonDocument>> cursor) {
        changeStreams.put(id, cursor);
    }

    public MongoCursor<ChangeStreamDocument<BsonDocument>> getChangeStream(final String id) {
        return getEntity(id, changeStreams, "change streams");
    }

    public boolean hasClient(final String id) {
        return clients.containsKey(id);
    }

    public MongoClient getClient(final String id) {
        return getEntity(id, clients, "client");
    }

    public boolean hasDatabase(final String id) {
        return databases.containsKey(id);
    }

    public MongoDatabase getDatabase(final String id) {
        return getEntity(id, databases, "database");
    }

    public boolean hasCollection(final String id) {
        return collections.containsKey(id);
    }

    public MongoCollection<BsonDocument> getCollection(final String id) {
        return getEntity(id, collections, "collection");
    }

    public ClientSession getSession(final String id) {
        return getEntity(id, sessions, "session");
    }

    public BsonDocument getSessionIdentifier(final String id) {
        return getEntity(id, sessionIdentifiers, "session identifier");
    }

    public GridFSBucket getBucket(final String id) {
        return getEntity(id, buckets, "bucket");
    }

    public TestCommandListener getClientCommandListener(final String id) {
        return getEntity(id, clientCommandListeners, "command listener");
    }

    private <T> T getEntity(final String id, final Map<String, T> entities, final String type) {
        T entity = entities.get(id);
        if (entity == null) {
            throw new IllegalStateException("Missing " + type + " with id: " + id);
        }
        return entity;
    }

    public void init(final BsonArray entitiesArray, final Function<MongoClientSettings, MongoClient> mongoClientSupplier) {
        for (BsonValue cur : entitiesArray.getValues()) {
            String entityType = cur.asDocument().getFirstKey();
            BsonDocument entity = cur.asDocument().getDocument(entityType);
            String id = entity.getString("id").getValue();
            switch (entityType) {
                case "client":
                    MongoClientSettings.Builder clientSettingsBuilder = getMongoClientSettingsBuilder();
                    if (entity.getBoolean("useMultipleMongoses", BsonBoolean.FALSE).getValue()) {
                        assumeTrue("Multiple mongos connection string not available for sharded cluster",
                                !isSharded() || getMultiMongosConnectionString() != null);
                        if (isSharded()) {
                            clientSettingsBuilder.applyConnectionString(requireNonNull(getMultiMongosConnectionString()));
                        }
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
                    clientSettingsBuilder.applyToServerSettings(builder -> {
                        builder.heartbeatFrequency(50, TimeUnit.MILLISECONDS);
                        builder.minHeartbeatFrequency(50, TimeUnit.MILLISECONDS);
                    });
                    if (entity.containsKey("uriOptions")) {
                        entity.getDocument("uriOptions").forEach((key, value) -> {
                            switch (key) {
                                case "retryReads":
                                   clientSettingsBuilder.retryReads(value.asBoolean().getValue());
                                   break;
                                case "retryWrites":
                                    clientSettingsBuilder.retryWrites(value.asBoolean().getValue());
                                    break;
                                case "readConcernLevel":
                                    clientSettingsBuilder.readConcern(
                                            new ReadConcern(ReadConcernLevel.fromString(value.asString().getValue())));
                                    break;
                                case "w":
                                    clientSettingsBuilder.writeConcern(new WriteConcern(value.asInt32().intValue()));
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unsupported uri option: " + key);
                            }
                        });
                    }
                    if (entity.containsKey("serverApi")) {
                        BsonDocument serverApiDocument = entity.getDocument("serverApi");
                        String apiVersion = serverApiDocument.getString("version").getValue();
                        ServerApi.Builder serverApiBuilder = ServerApi.builder().version(ServerApiVersion.findByValue(apiVersion));
                        if (serverApiDocument.containsKey("deprecationErrors")) {
                            serverApiBuilder.deprecationErrors(serverApiDocument.getBoolean("deprecationErrors").getValue());
                        }
                        if (serverApiDocument.containsKey("strict")) {
                            serverApiBuilder.strict(serverApiDocument.getBoolean("strict").getValue());
                        }
                        clientSettingsBuilder.serverApi(serverApiBuilder.build());
                    }
                    clients.put(id, mongoClientSupplier.apply(clientSettingsBuilder.build()));
                    break;
                case "database": {
                    MongoClient client = clients.get(entity.getString("client").getValue());
                    MongoDatabase database = client.getDatabase(entity.getString("databaseName").getValue());
                    if (entity.containsKey("collectionOptions")) {
                        for (Map.Entry<String, BsonValue> entry : entity.getDocument("collectionOptions").entrySet()) {
                            //noinspection SwitchStatementWithTooFewBranches
                            switch (entry.getKey()) {
                                case "readConcern":
                                    database = database.withReadConcern(asReadConcern(entry.getValue().asDocument()));
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
                                    collection = collection.withReadConcern(asReadConcern(entry.getValue().asDocument()));
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
                    ClientSessionOptions.Builder optionsBuilder = ClientSessionOptions.builder();
                    if (entity.containsKey("sessionOptions")) {
                        for (Map.Entry<String, BsonValue> entry : entity.getDocument("sessionOptions").entrySet()) {
                            //noinspection SwitchStatementWithTooFewBranches
                            switch (entry.getKey()) {
                                case "defaultTransactionOptions":
                                    optionsBuilder.defaultTransactionOptions(getTransactionOptions(entry.getValue().asDocument()));
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unsupported session option: " + entry.getKey());
                            }
                        }
                    }
                    ClientSession session = client.startSession(optionsBuilder.build());
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

    private TransactionOptions getTransactionOptions(final BsonDocument options) {
        TransactionOptions.Builder transactionOptionsBuilder = TransactionOptions.builder();
        for (Map.Entry<String, BsonValue> entry : options.entrySet()) {
            switch (entry.getKey()) {
                case "readConcern":
                    transactionOptionsBuilder.readConcern(asReadConcern(entry.getValue().asDocument()));
                    break;
                case "writeConcern":
                    transactionOptionsBuilder.writeConcern(asWriteConcern(entry.getValue().asDocument()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported transaction option: " + entry.getKey());
            }
        }
        return transactionOptionsBuilder.build();
    }

    public void close() {
        for (MongoClient client : clients.values()) {
            client.close();
        }
    }
}
