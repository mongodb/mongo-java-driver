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
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.assertions.Assertions;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.SynchronousContextProvider;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientWriteModel;
import com.mongodb.client.result.bulk.ClientBulkWriteResult;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.ClusterBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.TimeoutContext.createTimeoutContext;

final class MongoClusterImpl implements MongoCluster {
    @Nullable
    private final AutoEncryptionSettings autoEncryptionSettings;
    private final Cluster cluster;
    private final CodecRegistry codecRegistry;
    @Nullable
    private final SynchronousContextProvider contextProvider;
    @Nullable
    private final Crypt crypt;
    private final Object originator;
    private final OperationExecutor operationExecutor;
    private final ReadConcern readConcern;
    private final ReadPreference readPreference;
    private final boolean retryReads;
    private final boolean retryWrites;
    @Nullable
    private final ServerApi serverApi;
    private final ServerSessionPool serverSessionPool;
    private final TimeoutSettings timeoutSettings;
    private final UuidRepresentation uuidRepresentation;
    private final WriteConcern writeConcern;

    MongoClusterImpl(
            @Nullable final AutoEncryptionSettings autoEncryptionSettings, final Cluster cluster, final CodecRegistry codecRegistry,
            @Nullable final SynchronousContextProvider contextProvider, @Nullable final Crypt crypt, final Object originator,
            @Nullable final OperationExecutor operationExecutor, final ReadConcern readConcern, final ReadPreference readPreference,
            final boolean retryReads, final boolean retryWrites, @Nullable final ServerApi serverApi,
            final ServerSessionPool serverSessionPool, final TimeoutSettings timeoutSettings, final UuidRepresentation uuidRepresentation,
            final WriteConcern writeConcern) {
        this.autoEncryptionSettings = autoEncryptionSettings;
        this.cluster = cluster;
        this.codecRegistry = codecRegistry;
        this.contextProvider = contextProvider;
        this.crypt = crypt;
        this.originator = originator;
        this.operationExecutor = operationExecutor != null ? operationExecutor : new OperationExecutorImpl(timeoutSettings);
        this.readConcern = readConcern;
        this.readPreference = readPreference;
        this.retryReads = retryReads;
        this.retryWrites = retryWrites;
        this.serverApi = serverApi;
        this.serverSessionPool = serverSessionPool;
        this.timeoutSettings = timeoutSettings;
        this.uuidRepresentation = uuidRepresentation;
        this.writeConcern = writeConcern;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    @Override
    public Long getTimeout(final TimeUnit timeUnit) {
        Long timeoutMS = timeoutSettings.getTimeoutMS();
        return timeoutMS == null ? null : timeUnit.convert(timeoutMS, TimeUnit.MILLISECONDS);
    }

    @Override
    public MongoCluster withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoClusterImpl(autoEncryptionSettings, cluster, codecRegistry, contextProvider, crypt, originator,
                operationExecutor, readConcern, readPreference, retryReads, retryWrites, serverApi, serverSessionPool, timeoutSettings,
                uuidRepresentation, writeConcern);
    }

    @Override
    public MongoCluster withReadPreference(final ReadPreference readPreference) {
        return new MongoClusterImpl(autoEncryptionSettings, cluster, codecRegistry, contextProvider, crypt, originator,
                operationExecutor, readConcern, readPreference, retryReads, retryWrites, serverApi, serverSessionPool, timeoutSettings,
                uuidRepresentation, writeConcern);
    }

    @Override
    public MongoCluster withWriteConcern(final WriteConcern writeConcern) {
        return new MongoClusterImpl(autoEncryptionSettings, cluster, codecRegistry, contextProvider, crypt, originator,
                operationExecutor, readConcern, readPreference, retryReads, retryWrites, serverApi, serverSessionPool, timeoutSettings,
                uuidRepresentation, writeConcern);
    }

    @Override
    public MongoCluster withReadConcern(final ReadConcern readConcern) {
        return new MongoClusterImpl(autoEncryptionSettings, cluster, codecRegistry, contextProvider, crypt, originator,
                operationExecutor, readConcern, readPreference, retryReads, retryWrites, serverApi, serverSessionPool, timeoutSettings,
                uuidRepresentation, writeConcern);
    }

    @Override
    public MongoCluster withTimeout(final long timeout, final TimeUnit timeUnit) {
        return new MongoClusterImpl(autoEncryptionSettings, cluster, codecRegistry, contextProvider, crypt, originator,
                operationExecutor, readConcern, readPreference, retryReads, retryWrites, serverApi, serverSessionPool,
                timeoutSettings.withTimeout(timeout, timeUnit), uuidRepresentation, writeConcern);
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return new MongoDatabaseImpl(databaseName, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern,
                uuidRepresentation, autoEncryptionSettings, timeoutSettings, operationExecutor);
    }

    public Cluster getCluster() {
        return cluster;
    }

    @Nullable
    public Crypt getCrypt() {
        return crypt;
    }

    public OperationExecutor getOperationExecutor() {
        return operationExecutor;
    }

    public ServerSessionPool getServerSessionPool() {
        return serverSessionPool;
    }

    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    @Override
    public ClientSession startSession() {
        return startSession(ClientSessionOptions
                .builder()
                .defaultTransactionOptions(TransactionOptions.builder()
                        .readConcern(readConcern)
                        .writeConcern(writeConcern)
                        .build())
                .build());
    }

    @Override
    public ClientSession startSession(final ClientSessionOptions options) {
            notNull("options", options);

            ClientSessionOptions mergedOptions = ClientSessionOptions.builder(options)
                    .defaultTransactionOptions(
                            TransactionOptions.merge(
                                    options.getDefaultTransactionOptions(),
                                    TransactionOptions.builder()
                                            .readConcern(readConcern)
                                            .writeConcern(writeConcern)
                                            .readPreference(readPreference)
                                            .build()))
                    .build();
            return new ClientSessionImpl(serverSessionPool, originator, mergedOptions, operationExecutor);
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        return createListDatabaseNamesIterable(null);
    }

    @Override
    public MongoIterable<String> listDatabaseNames(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createListDatabaseNamesIterable(clientSession);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final Class<TResult> clazz) {
        return createListDatabasesIterable(null, clazz);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final ClientSession clientSession, final Class<TResult> clazz) {
        notNull("clientSession", clientSession);
        return createListDatabasesIterable(clientSession, clazz);
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return watch(Collections.emptyList());
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> clazz) {
        return watch(Collections.emptyList(), clazz);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> clazz) {
        return createChangeStreamIterable(null, pipeline, clazz);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Collections.emptyList());
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> clazz) {
        return watch(clientSession, Collections.emptyList(), clazz);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
            final Class<TResult> clazz) {
        notNull("clientSession", clientSession);
        return createChangeStreamIterable(clientSession, pipeline, clazz);
    }

    @Override
    public <TDocument> ClientBulkWriteResult bulkWrite(
            final List<? extends ClientWriteModel<? extends TDocument>> clientWriteModels,
            final Class<TDocument> tDocumentClass) throws ClientBulkWriteException {
        notNull("clientWriteModels", clientWriteModels);
        notNull("tDocumentClass", tDocumentClass);
        throw Assertions.fail("BULK-TODO implement");
    }

    @Override
    public <TDocument> ClientBulkWriteResult bulkWrite(
            final List<? extends ClientWriteModel<? extends TDocument>> clientWriteModels,
            final ClientBulkWriteOptions options,
            final Class<TDocument> tDocumentClass) throws ClientBulkWriteException {
        notNull("clientWriteModels", clientWriteModels);
        notNull("options", options);
        notNull("tDocumentClass", tDocumentClass);
        throw Assertions.fail("BULK-TODO implement");
    }

    @Override
    public <TDocument> ClientBulkWriteResult bulkWrite(
            final ClientSession clientSession,
            final List<? extends ClientWriteModel<? extends TDocument>> clientWriteModels,
            final Class<TDocument> tDocumentClass) throws ClientBulkWriteException {
        notNull("clientSession", clientSession);
        notNull("clientWriteModels", clientWriteModels);
        notNull("tDocumentClass", tDocumentClass);
        throw Assertions.fail("BULK-TODO implement");
    }

    @Override
    public <TDocument> ClientBulkWriteResult bulkWrite(
            final ClientSession clientSession,
            final List<? extends ClientWriteModel<? extends TDocument>> clientWriteModels,
            final ClientBulkWriteOptions options,
            final Class<TDocument> tDocumentClass) throws ClientBulkWriteException {
        notNull("clientSession", clientSession);
        notNull("clientWriteModels", clientWriteModels);
        notNull("options", options);
        notNull("tDocumentClass", tDocumentClass);
        throw Assertions.fail("BULK-TODO implement");
    }

    private <T> ListDatabasesIterable<T> createListDatabasesIterable(@Nullable final ClientSession clientSession, final Class<T> clazz) {
        return new ListDatabasesIterableImpl<>(clientSession, clazz, codecRegistry, ReadPreference.primary(), operationExecutor, retryReads, timeoutSettings);
    }

    private MongoIterable<String> createListDatabaseNamesIterable(@Nullable final ClientSession clientSession) {
        return createListDatabasesIterable(clientSession, BsonDocument.class)
                .nameOnly(true)
                .map(result -> result.getString("name").getValue());
    }

    private <TResult> ChangeStreamIterable<TResult> createChangeStreamIterable(@Nullable final ClientSession clientSession,
            final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new ChangeStreamIterableImpl<>(clientSession, "admin", codecRegistry, readPreference,
                readConcern, operationExecutor, pipeline, resultClass, ChangeStreamLevel.CLIENT,
                retryReads, timeoutSettings);
    }

    final class OperationExecutorImpl implements OperationExecutor {
        private final TimeoutSettings executorTimeoutSettings;

        OperationExecutorImpl(final TimeoutSettings executorTimeoutSettings) {
            this.executorTimeoutSettings = executorTimeoutSettings;
        }

        @Override
        public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern) {
            return execute(operation, readPreference, readConcern, null);
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation, final ReadConcern readConcern) {
            return execute(operation, readConcern, null);
        }

        @Override
        public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
                @Nullable final ClientSession session) {
            if (session != null) {
                session.notifyOperationInitiated(operation);
            }

            ClientSession actualClientSession = getClientSession(session);
            ReadBinding binding = getReadBinding(readPreference, readConcern, actualClientSession, session == null);

            try {
                if (actualClientSession.hasActiveTransaction() && !binding.getReadPreference().equals(primary())) {
                    throw new MongoClientException("Read preference in a transaction must be primary");
                }
                return operation.execute(binding);
            } catch (MongoException e) {
                labelException(actualClientSession, e);
                clearTransactionContextOnTransientTransactionError(session, e);
                throw e;
            } finally {
                binding.release();
            }
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation, final ReadConcern readConcern,
                @Nullable final ClientSession session) {
            if (session != null) {
                session.notifyOperationInitiated(operation);
            }

            ClientSession actualClientSession = getClientSession(session);
            WriteBinding binding = getWriteBinding(readConcern, actualClientSession, session == null);

            try {
                return operation.execute(binding);
            } catch (MongoException e) {
                labelException(actualClientSession, e);
                clearTransactionContextOnTransientTransactionError(session, e);
                throw e;
            } finally {
                binding.release();
            }
        }

        @Override
        public OperationExecutor withTimeoutSettings(final TimeoutSettings newTimeoutSettings) {
            if (Objects.equals(executorTimeoutSettings, newTimeoutSettings)) {
                return this;
            }
            return new OperationExecutorImpl(newTimeoutSettings);
        }

        @Override
        public TimeoutSettings getTimeoutSettings() {
            return executorTimeoutSettings;
        }

        WriteBinding getWriteBinding(final ReadConcern readConcern, final ClientSession session, final boolean ownsSession) {
            return getReadWriteBinding(primary(), readConcern, session, ownsSession);
        }

        ReadBinding getReadBinding(final ReadPreference readPreference, final ReadConcern readConcern, final ClientSession session,
                final boolean ownsSession) {
            return getReadWriteBinding(readPreference, readConcern, session, ownsSession);
        }

        ReadWriteBinding getReadWriteBinding(final ReadPreference readPreference,
                final ReadConcern readConcern, final ClientSession session, final boolean ownsSession) {

            ClusterAwareReadWriteBinding readWriteBinding = new ClusterBinding(cluster,
                    getReadPreferenceForBinding(readPreference, session), readConcern, getOperationContext(session, readConcern));

            if (crypt != null) {
                readWriteBinding = new CryptBinding(readWriteBinding, crypt);
            }

            return new ClientSessionBinding(session, ownsSession, readWriteBinding);
        }

        private OperationContext getOperationContext(final ClientSession session, final ReadConcern readConcern) {
            return new OperationContext(
                    getRequestContext(),
                    new ReadConcernAwareNoOpSessionContext(readConcern),
                    createTimeoutContext(session, executorTimeoutSettings),
                    serverApi);
        }

        private RequestContext getRequestContext() {
            RequestContext context = null;
            if (contextProvider != null) {
                context = contextProvider.getContext();
            }
            return context == null ? IgnorableRequestContext.INSTANCE : context;
        }

        private void labelException(final ClientSession session, final MongoException e) {
            if (session.hasActiveTransaction() && (e instanceof MongoSocketException || e instanceof MongoTimeoutException
                    || e instanceof MongoQueryException && e.getCode() == 91)
                    && !e.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                e.addLabel(TRANSIENT_TRANSACTION_ERROR_LABEL);
            }
        }

        private void clearTransactionContextOnTransientTransactionError(@Nullable final ClientSession session, final MongoException e) {
            if (session != null && e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                session.clearTransactionContext();
            }
        }

        private ReadPreference getReadPreferenceForBinding(final ReadPreference readPreference, @Nullable final ClientSession session) {
            if (session == null) {
                return readPreference;
            }
            if (session.hasActiveTransaction()) {
                ReadPreference readPreferenceForBinding = session.getTransactionOptions().getReadPreference();
                if (readPreferenceForBinding == null) {
                    throw new MongoInternalException("Invariant violated.  Transaction options read preference can not be null");
                }
                return readPreferenceForBinding;
            }
            return readPreference;
        }

        ClientSession getClientSession(@Nullable final ClientSession clientSessionFromOperation) {
            ClientSession session;
            if (clientSessionFromOperation != null) {
                isTrue("ClientSession from same MongoClient", clientSessionFromOperation.getOriginator() == originator);
                session = clientSessionFromOperation;
            } else {
                session = startSession(ClientSessionOptions.builder().
                        causallyConsistent(false)
                        .defaultTransactionOptions(
                                TransactionOptions.builder()
                                        .readConcern(ReadConcern.DEFAULT)
                                        .readPreference(ReadPreference.primary())
                                        .writeConcern(WriteConcern.ACKNOWLEDGED).build())
                        .build());
            }
            return session;
        }
    }
}
