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

package com.mongodb.client.internal;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientBulkWriteException;
import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.SynchronousContextProvider;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.SocketSettings;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.connection.ClientMetadata;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.DefaultClusterFactory;
import com.mongodb.internal.connection.InternalConnectionPoolSettings;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.internal.tracing.TracingManager;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.internal.Crypts.createCrypt;
import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;
import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class MongoClientImpl implements MongoClient {
    private static final Logger LOGGER = Loggers.getLogger("client");

    private final MongoClientSettings settings;
    private final MongoDriverInformation mongoDriverInformation;
    private final MongoClusterImpl delegate;
    private final AtomicBoolean closed;
    private final AutoCloseable externalResourceCloser;

    public MongoClientImpl(final Cluster cluster,
                           final MongoClientSettings settings,
                           final MongoDriverInformation mongoDriverInformation,
                           @Nullable final AutoCloseable externalResourceCloser) {
        this(cluster, mongoDriverInformation, settings, externalResourceCloser, null);
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    public MongoClientImpl(final Cluster cluster,
                            final MongoDriverInformation mongoDriverInformation,
                            final MongoClientSettings settings,
                            @Nullable final AutoCloseable externalResourceCloser,
                            @Nullable final OperationExecutor operationExecutor) {

        this.externalResourceCloser = externalResourceCloser;
        this.settings = notNull("settings", settings);
        this.mongoDriverInformation = mongoDriverInformation;
        AutoEncryptionSettings autoEncryptionSettings = settings.getAutoEncryptionSettings();
        if (settings.getContextProvider() != null && !(settings.getContextProvider() instanceof SynchronousContextProvider)) {
            throw new IllegalArgumentException("The contextProvider must be an instance of "
                    + SynchronousContextProvider.class.getName() + " when using the synchronous driver");
        }

        this.delegate = new MongoClusterImpl(autoEncryptionSettings, cluster,
                                             withUuidRepresentation(settings.getCodecRegistry(), settings.getUuidRepresentation()),
                                             (SynchronousContextProvider) settings.getContextProvider(),
                                             autoEncryptionSettings == null ? null : createCrypt(settings, autoEncryptionSettings), this,
                                             operationExecutor, settings.getReadConcern(), settings.getReadPreference(), settings.getRetryReads(),
                                             settings.getRetryWrites(), settings.getServerApi(),
                                             new ServerSessionPool(cluster, TimeoutSettings.create(settings), settings.getServerApi()),
                                             TimeoutSettings.create(settings), settings.getUuidRepresentation(),
                                             settings.getWriteConcern(), new TracingManager(settings.getObservabilitySettings()));
        this.closed = new AtomicBoolean();

        BsonDocument clientMetadataDocument = delegate.getCluster().getClientMetadata().getBsonDocument();
        LOGGER.info(format("MongoClient with metadata %s created with settings %s", clientMetadataDocument.toJson(), settings));
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            Crypt crypt = delegate.getCrypt();
            if (crypt != null) {
                crypt.close();
            }
            delegate.getServerSessionPool().close();
            delegate.getCluster().close();
            if (externalResourceCloser != null) {
                try {
                    externalResourceCloser.close();
                } catch (Exception e) {
                    LOGGER.warn("Exception closing resource", e);
                }
            }
        }
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return delegate.getCluster().getCurrentDescription();
    }

    @Override
    public void appendMetadata(final MongoDriverInformation mongoDriverInformation) {
        ClientMetadata clientMetadata = getCluster().getClientMetadata();
        clientMetadata.append(mongoDriverInformation);
        LOGGER.info(format("MongoClient metadata has been updated to %s", clientMetadata.getBsonDocument()));
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return delegate.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return delegate.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return delegate.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return delegate.getReadConcern();
    }

    @Override
    public Long getTimeout(final TimeUnit timeUnit) {
        return delegate.getTimeout(timeUnit);
    }

    @Override
    public MongoCluster withCodecRegistry(final CodecRegistry codecRegistry) {
        return delegate.withCodecRegistry(codecRegistry);
    }

    @Override
    public MongoCluster withReadPreference(final ReadPreference readPreference) {
        return delegate.withReadPreference(readPreference);
    }

    @Override
    public MongoCluster withWriteConcern(final WriteConcern writeConcern) {
        return delegate.withWriteConcern(writeConcern);
    }

    @Override
    public MongoCluster withReadConcern(final ReadConcern readConcern) {
        return delegate.withReadConcern(readConcern);
    }

    @Override
    public MongoCluster withTimeout(final long timeout, final TimeUnit timeUnit) {
        return delegate.withTimeout(timeout, timeUnit);
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public ClientSession startSession() {
        return delegate.startSession();
    }

    @Override
    public ClientSession startSession(final ClientSessionOptions options) {
        return delegate.startSession(options);
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        return delegate.listDatabaseNames();
    }

    @Override
    public MongoIterable<String> listDatabaseNames(final ClientSession clientSession) {
        return delegate.listDatabaseNames(clientSession);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return delegate.listDatabases();
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        return delegate.listDatabases(clientSession);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final Class<TResult> resultClass) {
        return delegate.listDatabases(resultClass);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final ClientSession clientSession, final Class<TResult> resultClass) {
        return delegate.listDatabases(clientSession, resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return delegate.watch();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return delegate.watch(resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return delegate.watch(pipeline);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return delegate.watch(pipeline, resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
        return delegate.watch(clientSession);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return delegate.watch(clientSession, resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return delegate.watch(clientSession, pipeline);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(
            final ClientSession clientSession, final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return delegate.watch(clientSession, pipeline, resultClass);
    }

    @Override
    public ClientBulkWriteResult bulkWrite(
            final List<? extends ClientNamespacedWriteModel> clientWriteModels) throws ClientBulkWriteException {
        return delegate.bulkWrite(clientWriteModels);
    }

    @Override
    public ClientBulkWriteResult bulkWrite(
            final List<? extends ClientNamespacedWriteModel> clientWriteModels,
            final ClientBulkWriteOptions options) throws ClientBulkWriteException {
        return delegate.bulkWrite(clientWriteModels, options);
    }

    @Override
    public ClientBulkWriteResult bulkWrite(
            final ClientSession clientSession,
            final List<? extends ClientNamespacedWriteModel> clientWriteModels) throws ClientBulkWriteException {
        return delegate.bulkWrite(clientSession, clientWriteModels);
    }

    @Override
    public ClientBulkWriteResult bulkWrite(
            final ClientSession clientSession,
            final List<? extends ClientNamespacedWriteModel> clientWriteModels,
            final ClientBulkWriteOptions options) throws ClientBulkWriteException {
        return delegate.bulkWrite(clientSession, clientWriteModels, options);
    }

    private static Cluster createCluster(final MongoClientSettings settings,
                                         @Nullable final MongoDriverInformation mongoDriverInformation,
                                         final StreamFactory streamFactory, final StreamFactory heartbeatStreamFactory) {
        notNull("settings", settings);
        return new DefaultClusterFactory().createCluster(settings.getClusterSettings(), settings.getServerSettings(),
                settings.getConnectionPoolSettings(), InternalConnectionPoolSettings.builder().build(),
                TimeoutSettings.create(settings), streamFactory,
                TimeoutSettings.createHeartbeatSettings(settings), heartbeatStreamFactory,
                settings.getCredential(), settings.getLoggerSettings(), getCommandListener(settings.getCommandListeners()),
                settings.getApplicationName(), mongoDriverInformation, settings.getCompressorList(), settings.getServerApi(),
                settings.getDnsClient());
    }

    private static StreamFactory getStreamFactory(
            final StreamFactoryFactory streamFactoryFactory,
            final MongoClientSettings settings,
            final boolean isHeartbeat) {
        SocketSettings socketSettings = isHeartbeat ? settings.getHeartbeatSocketSettings() : settings.getSocketSettings();
        return streamFactoryFactory.create(socketSettings, settings.getSslSettings());
    }

    public Cluster getCluster() {
        return delegate.getCluster();
    }

    public ServerSessionPool getServerSessionPool() {
        return delegate.getServerSessionPool();
    }

    public OperationExecutor getOperationExecutor() {
        return delegate.getOperationExecutor();
    }

    public TimeoutSettings getTimeoutSettings() {
        return delegate.getTimeoutSettings();
    }

    public MongoClientSettings getSettings() {
        return settings;
    }

    public MongoDriverInformation getMongoDriverInformation() {
        return mongoDriverInformation;
    }
}
