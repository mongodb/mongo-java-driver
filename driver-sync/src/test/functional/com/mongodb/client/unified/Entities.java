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

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.assertions.Assertions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckOutStartedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolReadyEvent;
import com.mongodb.event.ConnectionReadyEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.internal.connection.TestServerListener;
import com.mongodb.internal.logging.StructuredLogMessage;
import com.mongodb.lang.NonNull;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.ClusterFixture.isLoadBalanced;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.Fixture.getMultiMongosMongoClientSettingsBuilder;
import static com.mongodb.client.unified.EventMatcher.getReasonString;
import static com.mongodb.client.unified.UnifiedClientEncryptionHelper.createKmsProvidersMap;
import static com.mongodb.client.unified.UnifiedCrudHelper.asReadConcern;
import static com.mongodb.client.unified.UnifiedCrudHelper.asReadPreference;
import static com.mongodb.client.unified.UnifiedCrudHelper.asWriteConcern;
import static com.mongodb.internal.connection.AbstractConnectionPoolTest.waitForPoolAsyncWorkManagerStart;
import static java.util.Arrays.asList;
import static java.util.Collections.synchronizedList;
import static org.junit.Assume.assumeTrue;

public final class Entities {
    private static final Set<String> SUPPORTED_CLIENT_ENTITY_OPTIONS = new HashSet<>(
            asList(
                    "id", "uriOptions", "serverApi", "useMultipleMongoses", "storeEventsAsEntities",
                    "observeEvents", "observeLogMessages", "observeSensitiveCommands", "ignoreCommandMonitoringEvents"));
    private final Set<String> entityNames = new HashSet<>();
    private final Map<String, ExecutorService> threads = new HashMap<>();
    private final Map<String, ArrayList<Future<?>>> tasks = new HashMap<>();
    private final Map<String, BsonValue> results = new HashMap<>();
    private final Map<String, MongoClient> clients = new HashMap<>();
    private final Map<String, MongoDatabase> databases = new HashMap<>();
    private final Map<String, MongoCollection<BsonDocument>> collections = new HashMap<>();
    private final Map<String, ClientSession> sessions = new HashMap<>();
    private final Map<String, BsonDocument> sessionIdentifiers = new HashMap<>();
    private final Map<String, GridFSBucket> buckets = new HashMap<>();
    private final Map<String, ClientEncryption> clientEncryptions = new HashMap<>();
    private final Map<String, TestCommandListener> clientCommandListeners = new HashMap<>();
    private final Map<String, TestLoggingInterceptor> clientLoggingInterceptors = new HashMap<>();
    private final Map<String, TestConnectionPoolListener> clientConnectionPoolListeners = new HashMap<>();
    private final Map<String, TestServerListener> clientServerListeners = new HashMap<>();
    private final Map<String, MongoCursor<BsonDocument>> cursors = new HashMap<>();
    private final Map<String, ClusterDescription> topologyDescriptions = new HashMap<>();
    private final Map<String, Long> successCounts = new HashMap<>();
    private final Map<String, Long> iterationCounts = new HashMap<>();
    private final Map<String, BsonArray> errorDocumentsMap = new HashMap<>();
    private final Map<String, BsonArray> failureDocumentsMap = new HashMap<>();
    private final Map<String, List<BsonDocument>> eventsMap = new HashMap<>();

    public boolean hasSuccessCount(final String id) {
        return successCounts.containsKey(id);
    }

    public void addSuccessCount(final String id, final long count) {
        putEntity(id, count, successCounts);
    }

    public Long getSuccessCount(final String id) {
        return getEntity(id, successCounts, "successCount");
    }

    public boolean hasIterationCount(final String id) {
        return iterationCounts.containsKey(id);
    }

    public void addIterationCount(final String id, final long count) {
        putEntity(id, count, iterationCounts);
    }

    public Long getIterationCount(final String id) {
        return getEntity(id, iterationCounts, "successCount");
    }

    public boolean hasErrorDocuments(final String id) {
        return errorDocumentsMap.containsKey(id);
    }

    public void addErrorDocuments(final String id, final BsonArray errorDocuments) {
        putEntity(id, errorDocuments, errorDocumentsMap);
    }

    public BsonArray getErrorDocuments(final String id) {
        return getEntity(id, errorDocumentsMap, "errorDocuments");
    }

    public boolean hasFailureDocuments(final String id) {
        return failureDocumentsMap.containsKey(id);
    }

    public void addFailureDocuments(final String id, final BsonArray failureDocuments) {
        putEntity(id, failureDocuments, failureDocumentsMap);
    }

    public BsonArray getFailureDocuments(final String id) {
        return getEntity(id, failureDocumentsMap, "failureDocuments");
    }

    public boolean hasEvents(final String id) {
        return eventsMap.containsKey(id);
    }

    public List<BsonDocument> getEvents(final String id) {
        return getEntity(id, eventsMap, "events");
    }

    public void addResult(final String id, final BsonValue result) {
        putEntity(id, result, results);
    }

    public BsonValue getResult(final String id) {
        return getEntity(id, results, "result");
    }

    public void addCursor(final String id, final MongoCursor<BsonDocument> cursor) {
        putEntity(id, cursor, cursors);
    }

    public MongoCursor<BsonDocument> getCursor(final String id) {
        return getEntity(id, cursors, "cursors");
    }

    public void addTopologyDescription(final String id, final ClusterDescription clusterDescription) {
        putEntity(id, clusterDescription, topologyDescriptions);
    }

    public ClusterDescription getTopologyDescription(final String id) {
        return getEntity(id, topologyDescriptions, "topologyDescription");
    }

    public ExecutorService getThread(final String id) {
        return getEntity(id, threads, "thread");
    }

    public void addThreadTask(final String id, final Future<?> task) {
        getEntity(id, tasks, "tasks").add(task);
    }

    public List<Future<?>> getThreadTasks(final String id) {
        return getEntity(id, tasks, "tasks");
    }

    public void clearThreadTasks(final String id) {
        getEntity(id, tasks, "tasks").clear();
    }

    public boolean hasClient(final String id) {
        return clients.containsKey(id);
    }

    public MongoClient getClient(final String id) {
        return getEntity(id, clients, "client");
    }

    public ClientEncryption getClientEncryption(final String id) {
        return getEntity(id, clientEncryptions, "clientEncryption");
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
        return getEntity(id + "-identifier", sessionIdentifiers, "session identifier");
    }

    public GridFSBucket getBucket(final String id) {
        return getEntity(id, buckets, "bucket");
    }

    public TestCommandListener getClientCommandListener(final String id) {
        return getEntity(id + "-command-listener", clientCommandListeners, "command listener");
    }

    public TestLoggingInterceptor getClientLoggingInterceptor(final String id) {
        return getEntity(id + "-logging-interceptor", clientLoggingInterceptors, "logging interceptor");
    }

    public TestConnectionPoolListener getConnectionPoolListener(final String id) {
        return getEntity(id + "-connection-pool-listener", clientConnectionPoolListeners, "connection pool listener");
    }

    public TestServerListener getServerListener(final String id) {
        return getEntity(id + "-server-listener", clientServerListeners, "server listener");
    }

    private <T> T getEntity(final String id, final Map<String, T> entities, final String type) {
        T entity = entities.get(id);
        if (entity == null) {
            throw new IllegalStateException("Missing " + type + " with id: " + id);
        }
        return entity;
    }

    private <T> void putEntity(final String id, final T entity, final Map<String, T> entities) {
        if (!entityNames.add(id)) {
            throw new IllegalStateException("Already an entity with id: " + id);
        }
        entities.put(id, entity);
    }

    public void init(final BsonArray entitiesArray,
                     final boolean waitForPoolAsyncWorkManagerStart,
                     final Function<MongoClientSettings, MongoClient> mongoClientSupplier,
                     final Function<MongoDatabase, GridFSBucket> gridFSBucketSupplier,
                     final BiFunction<MongoClient, ClientEncryptionSettings, ClientEncryption> clientEncryptionSupplier) {
        for (BsonValue cur : entitiesArray.getValues()) {
            String entityType = cur.asDocument().getFirstKey();
            BsonDocument entity = cur.asDocument().getDocument(entityType);
            String id = entity.getString("id").getValue();
            switch (entityType) {
                case "thread":
                    initThread(id);
                    break;
                case "client":
                    initClient(entity, id, mongoClientSupplier, waitForPoolAsyncWorkManagerStart);
                    break;
                case "database": {
                    initDatabase(entity, id);
                    break;
                }
                case "collection": {
                    initCollection(entity, id);
                    break;
                }
                case "session": {
                    initSession(entity, id);
                    break;
                }
                case "bucket": {
                    initBucket(entity, id, gridFSBucketSupplier);
                    break;
                }
                case "clientEncryption": {
                    initClientEncryption(entity, id, clientEncryptionSupplier);
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unsupported entity type: " + entityType);
            }
        }
    }

    private void initThread(final String id) {
        putEntity(id, Executors.newSingleThreadExecutor(), threads);
        tasks.put(id, new ArrayList<>());
    }

    private void initClient(final BsonDocument entity, final String id,
                            final Function<MongoClientSettings, MongoClient> mongoClientSupplier,
                            final boolean waitForPoolAsyncWorkManagerStart) {
        if (!SUPPORTED_CLIENT_ENTITY_OPTIONS.containsAll(entity.keySet())) {
            throw new UnsupportedOperationException("Client entity contains unsupported options: " + entity.keySet()
                    + ". Supported options are " + SUPPORTED_CLIENT_ENTITY_OPTIONS);
        }
        MongoClientSettings.Builder clientSettingsBuilder;
        if (entity.getBoolean("useMultipleMongoses", BsonBoolean.FALSE).getValue() && (isSharded() || isLoadBalanced())) {
            assumeTrue("Multiple mongos connection string not available for sharded cluster",
                    !isSharded() || getMultiMongosConnectionString() != null);
            assumeTrue("Multiple mongos connection string not available for load-balanced cluster",
                    !isLoadBalanced() || getMultiMongosConnectionString() != null);
            clientSettingsBuilder = getMultiMongosMongoClientSettingsBuilder();
        } else {
            clientSettingsBuilder = getMongoClientSettingsBuilder();
        }

        clientSettingsBuilder.applicationName(id);
        clientSettingsBuilder.applyToLoggerSettings(builder -> builder.maxDocumentLength(10_000));

        TestServerListener testClusterListener = new TestServerListener();
        clientSettingsBuilder.applyToServerSettings(builder -> builder.addServerListener(testClusterListener));
        putEntity(id + "-server-listener", testClusterListener, clientServerListeners);

        if (entity.containsKey("observeEvents")) {
            List<String> ignoreCommandMonitoringEvents = entity
                    .getArray("ignoreCommandMonitoringEvents", new BsonArray()).stream()
                    .map(type -> type.asString().getValue()).collect(Collectors.toList());
            ignoreCommandMonitoringEvents.add("configureFailPoint");
            TestCommandListener testCommandListener = new TestCommandListener(
                    entity.getArray("observeEvents").stream()
                            .map(type -> type.asString().getValue()).collect(Collectors.toList()),
                    ignoreCommandMonitoringEvents, entity.getBoolean("observeSensitiveCommands", BsonBoolean.FALSE).getValue());
            clientSettingsBuilder.addCommandListener(testCommandListener);
            putEntity(id + "-command-listener", testCommandListener, clientCommandListeners);

            TestConnectionPoolListener testConnectionPoolListener = new TestConnectionPoolListener(
                    entity.getArray("observeEvents").stream()
                            .map(type -> type.asString().getValue()).collect(Collectors.toList()));
            clientSettingsBuilder.applyToConnectionPoolSettings(builder ->
                    builder.addConnectionPoolListener(testConnectionPoolListener));
            putEntity(id + "-connection-pool-listener", testConnectionPoolListener, clientConnectionPoolListeners);
        } else {
            // Regardless of whether events are observed, we still need to track some info about the pool in order to implement
            // the assertNumberConnectionsCheckedOut operation
            TestConnectionPoolListener testConnectionPoolListener = new TestConnectionPoolListener();
            clientSettingsBuilder.applyToConnectionPoolSettings(builder ->
                    builder.addConnectionPoolListener(testConnectionPoolListener));
            putEntity(id + "-connection-pool-listener", testConnectionPoolListener, clientConnectionPoolListeners);
        }

        if (entity.containsKey("storeEventsAsEntities")) {
            BsonArray storeEventsAsEntitiesArray = entity.getArray("storeEventsAsEntities");
            for (BsonValue eventValue : storeEventsAsEntitiesArray) {
                BsonDocument eventDocument = eventValue.asDocument();
                String key = eventDocument.getString("id").getValue();
                BsonArray eventList = eventDocument.getArray("events");
                List<BsonDocument> eventDocumentList = synchronizedList(new ArrayList<>());
                putEntity(key, eventDocumentList, eventsMap);

                if (eventList.stream()
                        .map(value -> value.asString().getValue())
                        .anyMatch(value -> value.startsWith("Command"))) {
                    clientSettingsBuilder.addCommandListener(new EntityCommandListener(eventList.stream()
                            .map(value -> value.asString().getValue())
                            .collect(Collectors.toSet()),
                            eventDocumentList));
                }
                if (eventList.stream()
                        .map(value -> value.asString().getValue())
                        .anyMatch(value -> value.startsWith("Pool") || value.startsWith("Connection"))) {
                    clientSettingsBuilder.
                            applyToConnectionPoolSettings(builder ->
                                    builder.addConnectionPoolListener(new EntityConnectionPoolListener(eventList.stream()
                                            .map(value -> value.asString().getValue())
                                            .collect(Collectors.toSet()),
                                            eventDocumentList)));
                }
            }
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
                        if (value.isString()) {
                            clientSettingsBuilder.writeConcern(new WriteConcern(value.asString().getValue()));
                        } else {
                            clientSettingsBuilder.writeConcern(new WriteConcern(value.asInt32().intValue()));
                        }
                        break;
                    case "maxPoolSize":
                        clientSettingsBuilder.applyToConnectionPoolSettings(builder -> builder.maxSize(value.asNumber().intValue()));
                        break;
                    case "minPoolSize":
                        clientSettingsBuilder.applyToConnectionPoolSettings(builder -> builder.minSize(value.asNumber().intValue()));
                        break;
                    case "waitQueueTimeoutMS":
                        clientSettingsBuilder.applyToConnectionPoolSettings(builder ->
                                builder.maxWaitTime(value.asNumber().longValue(), TimeUnit.MILLISECONDS));
                        break;
                    case "maxIdleTimeMS":
                        clientSettingsBuilder.applyToConnectionPoolSettings(builder ->
                                builder.maxConnectionIdleTime(value.asNumber().longValue(), TimeUnit.MILLISECONDS));
                        break;
                    case "maxConnecting":
                        clientSettingsBuilder.applyToConnectionPoolSettings(builder ->
                                builder.maxConnecting(value.asNumber().intValue()));
                        break;
                    case "heartbeatFrequencyMS":
                        clientSettingsBuilder.applyToServerSettings(builder ->
                                builder.heartbeatFrequency(value.asNumber().longValue(), TimeUnit.MILLISECONDS));
                        break;
                    case "connectTimeoutMS":
                        clientSettingsBuilder.applyToSocketSettings(builder ->
                                builder.connectTimeout(value.asNumber().intValue(), TimeUnit.MILLISECONDS));
                        break;
                    case "socketTimeoutMS":
                        clientSettingsBuilder.applyToSocketSettings(builder ->
                                builder.readTimeout(value.asNumber().intValue(), TimeUnit.MILLISECONDS));
                        break;
                    case "serverSelectionTimeoutMS":
                        clientSettingsBuilder.applyToClusterSettings(builder ->
                                builder.serverSelectionTimeout(value.asNumber().longValue(), TimeUnit.MILLISECONDS));
                        break;
                    case "loadBalanced":
                        if (value.asBoolean().getValue()) {
                            clientSettingsBuilder.applyToClusterSettings(builder -> builder.mode(ClusterConnectionMode.LOAD_BALANCED));
                        }
                        break;
                    case "appname":
                        clientSettingsBuilder.applicationName(value.asString().getValue());
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
        MongoClientSettings clientSettings = clientSettingsBuilder.build();

        if (entity.containsKey("observeLogMessages")) {
            BsonDocument observeLogMessagesDocument = entity.getDocument("observeLogMessages");
            TestLoggingInterceptor.LoggingFilter loggingFilter = new TestLoggingInterceptor.LoggingFilter();

            observeLogMessagesDocument.forEach((componentName, bsonValue) -> {
                StructuredLogMessage.Component component = StructuredLogMessage.Component
                        .valueOf(componentName.toUpperCase());

                String levelName = bsonValue
                        .asString()
                        .getValue()
                        .toUpperCase();
                StructuredLogMessage.Level level = StructuredLogMessage.Level.valueOf(levelName);

                loggingFilter.addComponent(component, level);

            });
            putEntity(id + "-logging-interceptor", new TestLoggingInterceptor(clientSettings.getApplicationName(), loggingFilter),
                    clientLoggingInterceptors);
        }

        putEntity(id, mongoClientSupplier.apply(clientSettings), clients);
        if (waitForPoolAsyncWorkManagerStart) {
            waitForPoolAsyncWorkManagerStart();
        }
    }

    private void initDatabase(final BsonDocument entity, final String id) {
        MongoClient client = clients.get(entity.getString("client").getValue());
        MongoDatabase database = client.getDatabase(entity.getString("databaseName").getValue());
        if (entity.containsKey("databaseOptions")) {
            for (Map.Entry<String, BsonValue> entry : entity.getDocument("databaseOptions").entrySet()) {
                switch (entry.getKey()) {
                    case "readConcern":
                        database = database.withReadConcern(asReadConcern(entry.getValue().asDocument()));
                        break;
                    case "readPreference":
                        database = database.withReadPreference(asReadPreference(entry.getValue().asDocument()));
                        break;
                    case "writeConcern":
                        database = database.withWriteConcern(asWriteConcern(entry.getValue().asDocument()));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported database option: " + entry.getKey());
                }
            }
        }
        putEntity(id, database, databases);
    }

    private void initCollection(final BsonDocument entity, final String id) {
        MongoDatabase database = databases.get(entity.getString("database").getValue());
        MongoCollection<BsonDocument> collection = database.getCollection(entity.getString("collectionName").getValue(),
                BsonDocument.class);
        if (entity.containsKey("collectionOptions")) {
            for (Map.Entry<String, BsonValue> entry : entity.getDocument("collectionOptions").entrySet()) {
                switch (entry.getKey()) {
                    case "readConcern":
                        collection = collection.withReadConcern(asReadConcern(entry.getValue().asDocument()));
                        break;
                    case "readPreference":
                        collection = collection.withReadPreference(asReadPreference(entry.getValue().asDocument()));
                        break;
                    case "writeConcern":
                        collection = collection.withWriteConcern(asWriteConcern(entry.getValue().asDocument()));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported collection option: " + entry.getKey());
                }
            }
        }
        putEntity(id, collection, collections);
    }

    private void initSession(final BsonDocument entity, final String id) {
        MongoClient client = clients.get(entity.getString("client").getValue());
        ClientSessionOptions.Builder optionsBuilder = ClientSessionOptions.builder();
        if (entity.containsKey("sessionOptions")) {
            for (Map.Entry<String, BsonValue> entry : entity.getDocument("sessionOptions").entrySet()) {
                switch (entry.getKey()) {
                    case "defaultTransactionOptions":
                        optionsBuilder.defaultTransactionOptions(getTransactionOptions(entry.getValue().asDocument()));
                        break;
                    case "snapshot":
                        optionsBuilder.snapshot(entry.getValue().asBoolean().getValue());
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported session option: " + entry.getKey());
                }
            }
        }
        ClientSession session = client.startSession(optionsBuilder.build());
        putEntity(id, session, sessions);
        putEntity(id + "-identifier", session.getServerSession().getIdentifier(), sessionIdentifiers);
    }

    private void initBucket(final BsonDocument entity, final String id, final Function<MongoDatabase, GridFSBucket> gridFSBucketSupplier) {
        MongoDatabase database = databases.get(entity.getString("database").getValue());
        if (entity.containsKey("bucketOptions")) {
            throw new UnsupportedOperationException("Unsupported session specification: bucketOptions");
        }
        putEntity(id, gridFSBucketSupplier.apply(database), buckets);
    }

    private void initClientEncryption(final BsonDocument entity, final String id,
            final BiFunction<MongoClient, ClientEncryptionSettings, ClientEncryption> clientEncryptionSupplier) {
        if (!entity.containsKey("clientEncryptionOpts")) {
            throw new UnsupportedOperationException("Unsupported client encryption specification missing: clientEncryptionOpts");
        }
        BsonDocument clientEncryptionOpts = entity.getDocument("clientEncryptionOpts");
        if (!clientEncryptionOpts.containsKey("keyVaultClient")) {
            throw new UnsupportedOperationException("Unsupported client encryption specification missing: "
                    + "clientEncryptionOpts.keyVaultClient");
        }

        MongoClient mongoClient = null;
        ClientEncryptionSettings.Builder builder = ClientEncryptionSettings.builder();
        // this is ignored in preference to the keyVaultClient, but required to be non-null in the ClientEncryptionSettings constructor
        builder.keyVaultMongoClientSettings(MongoClientSettings.builder().build());
        for (Map.Entry<String, BsonValue> entry : clientEncryptionOpts.entrySet()) {
            switch (entry.getKey()) {
                case "keyVaultClient":
                    mongoClient = clients.get(entry.getValue().asString().getValue());
                    break;
                case "keyVaultNamespace":
                    builder.keyVaultNamespace(entry.getValue().asString().getValue());
                    break;
                case "kmsProviders":
                    builder.kmsProviders(createKmsProvidersMap(entry.getValue().asDocument()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported client encryption option: " + entry.getKey());
            }
        }

        putEntity(id, clientEncryptionSupplier.apply(Assertions.notNull("mongoClient", mongoClient), builder.build()), clientEncryptions);
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
        cursors.values().forEach(MongoCursor::close);
        sessions.values().forEach(ClientSession::close);
        clients.values().forEach(MongoClient::close);
        clientLoggingInterceptors.values().forEach(TestLoggingInterceptor::close);
        threads.values().forEach(ExecutorService::shutdownNow);
    }

    private static class EntityCommandListener implements CommandListener {
        private final List<BsonDocument> eventDocumentList;
        private final Set<String> enabledEvents;

        EntityCommandListener(final Set<String> enabledEvents, final List<BsonDocument> eventDocumentList) {
            this.eventDocumentList = eventDocumentList;
            this.enabledEvents = enabledEvents;
        }

        @Override
        public void commandStarted(final CommandStartedEvent event) {
            if (enabledEvents.contains("CommandStartedEvent")) {
                eventDocumentList.add(createEventDocument(event, "CommandStartedEvent")
                        .append("databaseName", new BsonString(event.getDatabaseName())));
            }
        }

        @Override
        public void commandSucceeded(final CommandSucceededEvent event) {
            if (enabledEvents.contains("CommandSucceededEvent")) {
                eventDocumentList.add(createEventDocument(event, "CommandSucceededEvent")
                        .append("duration", new BsonInt64(event.getElapsedTime(TimeUnit.MILLISECONDS))));
            }
        }

        @Override
        public void commandFailed(final CommandFailedEvent event) {
            if (enabledEvents.contains("CommandFailedEvent")) {
                eventDocumentList.add(createEventDocument(event, "CommandFailedEvent")
                        .append("duration",
                                new BsonDouble(event.getElapsedTime(TimeUnit.NANOSECONDS) / 1_000_000_000.0))
                        .append("failure", new BsonString(event.getThrowable().toString())));
            }
        }

        private BsonDocument createEventDocument(final CommandEvent event, final String name) {
            return new BsonDocument()
                    .append("name", new BsonString(name))
                    .append("observedAt", new BsonDouble(System.currentTimeMillis() / 1000.0))
                    .append("commandName", new BsonString(event.getCommandName()))
                    .append("requestId", new BsonInt32(event.getRequestId()));
        }
    }

    private static class EntityConnectionPoolListener implements ConnectionPoolListener {
        private final List<BsonDocument> eventDocumentList;
        private final Set<String> enabledEvents;

        EntityConnectionPoolListener(final Set<String> enabledEvents, final List<BsonDocument> eventDocumentList) {
            this.eventDocumentList = eventDocumentList;
            this.enabledEvents = enabledEvents;
        }

        @Override
        public void connectionPoolCreated(final ConnectionPoolCreatedEvent event) {
            if (enabledEvents.contains("PoolCreatedEvent")) {
                eventDocumentList.add(createEventDocument("PoolCreatedEvent", event.getServerId()));
            }
        }

        @Override
        public void connectionPoolCleared(final ConnectionPoolClearedEvent event) {
            if (enabledEvents.contains("PoolClearedEvent")) {
                eventDocumentList.add(createEventDocument("PoolClearedEvent", event.getServerId()));
            }
        }

        @Override
        public void connectionPoolReady(final ConnectionPoolReadyEvent event) {
            if (enabledEvents.contains("PoolReadyEvent")) {
                eventDocumentList.add(createEventDocument("PoolReadyEvent", event.getServerId()));
            }
        }

        @Override
        public void connectionPoolClosed(final ConnectionPoolClosedEvent event) {
            if (enabledEvents.contains("PoolClosedEvent")) {
                eventDocumentList.add(createEventDocument("PoolClosedEvent", event.getServerId()));
            }
        }

        @Override
        public void connectionCheckOutStarted(final ConnectionCheckOutStartedEvent event) {
            if (enabledEvents.contains("ConnectionCheckOutStartedEvent")) {
                eventDocumentList.add(createEventDocument("ConnectionCheckOutStartedEvent", event.getServerId()));
            }
        }

        @Override
        public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
            if (enabledEvents.contains("ConnectionCheckedOutEvent")) {
                eventDocumentList.add(createEventDocument("ConnectionCheckedOutEvent", event.getConnectionId()));
            }
        }

        @Override
        public void connectionCheckOutFailed(final ConnectionCheckOutFailedEvent event) {
            if (enabledEvents.contains("ConnectionCheckOutFailedEvent")) {
                eventDocumentList.add(createEventDocument("ConnectionCheckOutFailedEvent", event.getServerId())
                .append("reason", new BsonString(getReasonString(event.getReason()))));
            }
        }

        @Override
        public void connectionCheckedIn(final ConnectionCheckedInEvent event) {
            if (enabledEvents.contains("ConnectionCheckedInEvent")) {
                eventDocumentList.add(createEventDocument("ConnectionCheckedInEvent", event.getConnectionId()));
            }
        }

        @Override
        public void connectionCreated(final ConnectionCreatedEvent event) {
            if (enabledEvents.contains("ConnectionCreatedEvent")) {
                eventDocumentList.add(createEventDocument("ConnectionCreatedEvent", event.getConnectionId()));
            }
        }

        @Override
        public void connectionReady(final ConnectionReadyEvent event) {
            if (enabledEvents.contains("ConnectionReadyEvent")) {
                eventDocumentList.add(createEventDocument("ConnectionReadyEvent", event.getConnectionId()));
            }
        }

        @Override
        public void connectionClosed(final ConnectionClosedEvent event) {
            if (enabledEvents.contains("ConnectionClosedEvent")) {
                eventDocumentList.add(createEventDocument("ConnectionClosedEvent", event.getConnectionId())
                        .append("reason", new BsonString(getReasonString(event.getReason()))));
            }
        }

        private BsonDocument createEventDocument(final String name, final ConnectionId connectionId) {
            return createEventDocument(name, connectionId.getServerId())
                    .append("connectionId", new BsonString(Integer.toString(connectionId.getLocalValue())));
        }

        private BsonDocument createEventDocument(final String name, final ServerId serverId) {
            return new BsonDocument()
                    .append("name", new BsonString(name))
                    .append("observedAt", new BsonDouble(System.currentTimeMillis() / 1000.0))
                    .append("address", new BsonString(getAddressAsString(serverId)));
        }

        @NonNull
        private String getAddressAsString(final ServerId serverId) {
            return serverId.getAddress().getHost() + ":" + serverId.getAddress().getPort();
        }
    }
}
