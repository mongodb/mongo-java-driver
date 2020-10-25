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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;

final class Entities {
    private final Map<String, MongoClient> clients = new HashMap<>();
    private final Map<String, MongoDatabase> databases = new HashMap<>();
    private final Map<String, MongoCollection<BsonDocument>> collections = new HashMap<>();
    private final Map<String, ClientSession> sessions = new HashMap<>();
    private final Map<String, BsonDocument> sessionIdentifiers = new HashMap<>();
    private final Map<String, GridFSBucket> buckets = new HashMap<>();
    private final Map<String, TestCommandListener> clientCommandListeners = new HashMap<>();

    public MongoClient getClient(final String id) {
        return clients.get(id);
    }

    public MongoDatabase getDatabase(final String id) {
        return databases.get(id);
    }

    public MongoCollection<BsonDocument> getCollection(final String id) {
        return collections.get(id);
    }

    public ClientSession getSession(final String id) {
        return sessions.get(id);
    }

    public BsonDocument getSessionIdentifier(final String id) {
        return sessionIdentifiers.get(id);
    }

    public GridFSBucket getBucket(final String id) {
        return buckets.get(id);
    }

    public TestCommandListener getClientCommandListener(final String id) {
        return clientCommandListeners.get(id);
    }

    public void init(final BsonArray entitiesArray, final MongoClientSupplier mongoClientSupplier) {
        for (BsonValue cur : entitiesArray.getValues()) {
            String entityType = cur.asDocument().getFirstKey();
            BsonDocument entity = cur.asDocument().getDocument(entityType);
            String id = entity.getString("id").getValue();
            switch (entityType) {
                case "client":
                    MongoClientSettings.Builder clientSettingsBuilder = getMongoClientSettingsBuilder();
                    if (entity.containsKey("observeEvents")) {
                        TestCommandListener testCommandListener = new TestCommandListener(
                                entity.getArray("observeEvents").stream()
                                        .map(type -> type.asString().getValue()).collect(Collectors.toList()));
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
                    databases.put(id, client.getDatabase(entity.getString("databaseName").getValue()));
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
                    sessions.put(id, session);
                    sessionIdentifiers.put(id, session.getServerSession().getIdentifier());
                    break;
                }
                case "bucket": {
                    MongoDatabase database = databases.get(entity.getString("database").getValue());
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
