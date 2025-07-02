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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientSessionOptions;
import com.mongodb.ContextProvider;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.ClientMetadata;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCluster;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.ReactiveContextProvider;
import com.mongodb.reactivestreams.client.internal.crypt.Crypt;
import com.mongodb.reactivestreams.client.internal.crypt.Crypts;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation;


/**
 * The internal MongoClient implementation.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class MongoClientImpl implements MongoClient {

    private static final Logger LOGGER = Loggers.getLogger("client");
    private final MongoClientSettings settings;
    private final AutoCloseable externalResourceCloser;

    private final MongoClusterImpl delegate;
    private final AtomicBoolean closed;

    public MongoClientImpl(final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation, final Cluster cluster,
            @Nullable final AutoCloseable externalResourceCloser) {
        this(settings, mongoDriverInformation, cluster, null, externalResourceCloser);
    }

    public MongoClientImpl(final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation, final Cluster cluster,
            @Nullable final OperationExecutor executor) {
        this(settings, mongoDriverInformation, cluster, executor, null);
    }

    private MongoClientImpl(final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation, final Cluster cluster,
                            @Nullable final OperationExecutor executor, @Nullable final AutoCloseable externalResourceCloser) {
        notNull("settings", settings);
        notNull("cluster", cluster);

        TimeoutSettings timeoutSettings = TimeoutSettings.create(settings);
        ServerSessionPool serverSessionPool = new ServerSessionPool(cluster, timeoutSettings, settings.getServerApi());
        ClientSessionHelper clientSessionHelper = new ClientSessionHelper(this, serverSessionPool);

        AutoEncryptionSettings autoEncryptSettings = settings.getAutoEncryptionSettings();
        Crypt crypt = autoEncryptSettings != null ? Crypts.createCrypt(settings, autoEncryptSettings) : null;
        ContextProvider contextProvider = settings.getContextProvider();
        if (contextProvider != null && !(contextProvider instanceof ReactiveContextProvider)) {
            throw new IllegalArgumentException("The contextProvider must be an instance of "
                    + ReactiveContextProvider.class.getName() + " when using the Reactive Streams driver");
        }
        OperationExecutor operationExecutor = executor != null ? executor
                : new OperationExecutorImpl(this, clientSessionHelper, timeoutSettings, (ReactiveContextProvider) contextProvider);
        MongoOperationPublisher<Document> mongoOperationPublisher = new MongoOperationPublisher<>(Document.class,
                withUuidRepresentation(settings.getCodecRegistry(),
                        settings.getUuidRepresentation()),
                settings.getReadPreference(),
                settings.getReadConcern(), settings.getWriteConcern(),
                settings.getRetryWrites(), settings.getRetryReads(),
                settings.getUuidRepresentation(),
                settings.getAutoEncryptionSettings(),
                timeoutSettings,
                operationExecutor);

        this.delegate = new MongoClusterImpl(cluster, crypt, operationExecutor, serverSessionPool, clientSessionHelper,
                mongoOperationPublisher);
        this.externalResourceCloser = externalResourceCloser;
        this.settings = settings;
        this.closed = new AtomicBoolean();

        BsonDocument clientMetadataDocument = delegate.getCluster().getClientMetadata().getBsonDocument();
        LOGGER.info(format("MongoClient with metadata %s created with settings %s", clientMetadataDocument.toJson(), settings));
    }

    Cluster getCluster() {
        return delegate.getCluster();
    }

    public ServerSessionPool getServerSessionPool() {
        return delegate.getServerSessionPool();
    }

    MongoOperationPublisher<Document> getMongoOperationPublisher() {
        return delegate.getMongoOperationPublisher();
    }

    @Nullable
    Crypt getCrypt() {
        return delegate.getCrypt();
    }

    public MongoClientSettings getSettings() {
        return settings;
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            Crypt crypt = getCrypt();
            if (crypt != null) {
                crypt.close();
            }
            getServerSessionPool().close();
            getCluster().close();
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
    public Publisher<String> listDatabaseNames() {
        return delegate.listDatabaseNames();
    }

    @Override
    public Publisher<String> listDatabaseNames(final ClientSession clientSession) {
        return delegate.listDatabaseNames(clientSession);
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases() {
        return delegate.listDatabases();
    }

    @Override
    public <TResult> ListDatabasesPublisher<TResult> listDatabases(final Class<TResult> clazz) {
        return delegate.listDatabases(clazz);
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases(final ClientSession clientSession) {
        return delegate.listDatabases(clientSession);
    }

    @Override
    public <TResult> ListDatabasesPublisher<TResult> listDatabases(final ClientSession clientSession, final Class<TResult> clazz) {
        return delegate.listDatabases(clientSession, clazz);
    }

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return delegate.watch();
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final Class<TResult> resultClass) {
        return delegate.watch(resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final List<? extends Bson> pipeline) {
        return delegate.watch(pipeline);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return delegate.watch(pipeline, resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession) {
        return delegate.watch(clientSession);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return delegate.watch(clientSession, resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return delegate.watch(clientSession, pipeline);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(
            final ClientSession clientSession, final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return delegate.watch(clientSession, pipeline, resultClass);
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(final List<? extends ClientNamespacedWriteModel> models) {
        return delegate.bulkWrite(models);
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(final List<? extends ClientNamespacedWriteModel> models,
                                                      final ClientBulkWriteOptions options) {
        return delegate.bulkWrite(models, options);
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(final ClientSession clientSession,
                                                      final List<? extends ClientNamespacedWriteModel> models) {
        return delegate.bulkWrite(clientSession, models);
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(final ClientSession clientSession,
                                                      final List<? extends ClientNamespacedWriteModel> models,
                                                      final ClientBulkWriteOptions options) {
        return delegate.bulkWrite(clientSession, models, options);
    }

    @Override
    public Publisher<ClientSession> startSession() {
        return delegate.startSession();
    }

    @Override
    public Publisher<ClientSession> startSession(final ClientSessionOptions options) {
        return delegate.startSession(options);
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
        return null;
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
    public MongoDatabase getDatabase(final String name) {
        return delegate.getDatabase(name);
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return getCluster().getCurrentDescription();
    }

    @Override
    public void appendMetadata(final MongoDriverInformation mongoDriverInformation) {
        ClientMetadata clientMetadata = getCluster().getClientMetadata();
        clientMetadata.append(mongoDriverInformation);
        LOGGER.info(format("MongoClient metadata has been updated to %s", clientMetadata.getBsonDocument()));
    }
}
