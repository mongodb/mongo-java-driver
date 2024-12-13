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

import com.mongodb.ClientBulkWriteException;
import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.MongoCluster;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.internal.crypt.Crypt;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class MongoClusterImpl implements MongoCluster {

    private final Cluster cluster;
    private final Crypt crypt;
    private final OperationExecutor operationExecutor;
    private final ServerSessionPool serverSessionPool;
    private final ClientSessionHelper clientSessionHelper;
    private final MongoOperationPublisher<Document> mongoOperationPublisher;

    MongoClusterImpl(final Cluster cluster, @Nullable final Crypt crypt, final OperationExecutor operationExecutor,
            final ServerSessionPool serverSessionPool, final ClientSessionHelper clientSessionHelper,
            final MongoOperationPublisher<Document> mongoOperationPublisher) {

        this.cluster = cluster;
        this.crypt = crypt;
        this.operationExecutor = operationExecutor;
        this.serverSessionPool = serverSessionPool;
        this.clientSessionHelper = clientSessionHelper;
        this.mongoOperationPublisher = mongoOperationPublisher;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return mongoOperationPublisher.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return mongoOperationPublisher.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return mongoOperationPublisher.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return mongoOperationPublisher.getReadConcern();
    }

    @Override
    public Long getTimeout(final TimeUnit timeUnit) {
        Long timeoutMS = mongoOperationPublisher.getTimeoutMS();
        return timeoutMS != null ? MILLISECONDS.convert(timeoutMS, timeUnit) : null;
    }

    @Override
    public MongoCluster withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoClusterImpl(cluster, crypt, operationExecutor, serverSessionPool, clientSessionHelper,
                mongoOperationPublisher.withCodecRegistry(codecRegistry));
    }

    @Override
    public MongoCluster withReadPreference(final ReadPreference readPreference) {
        return new MongoClusterImpl(cluster, crypt, operationExecutor, serverSessionPool, clientSessionHelper,
                mongoOperationPublisher.withReadPreference(readPreference));
    }

    @Override
    public MongoCluster withWriteConcern(final WriteConcern writeConcern) {
        return new MongoClusterImpl(cluster, crypt, operationExecutor, serverSessionPool, clientSessionHelper,
                mongoOperationPublisher.withWriteConcern(writeConcern));
    }

    @Override
    public MongoCluster withReadConcern(final ReadConcern readConcern) {
        return new MongoClusterImpl(cluster, crypt, operationExecutor, serverSessionPool, clientSessionHelper,
                mongoOperationPublisher.withReadConcern(readConcern));
    }

    @Override
    public MongoCluster withTimeout(final long timeout, final TimeUnit timeUnit) {
        return new MongoClusterImpl(cluster, crypt, operationExecutor, serverSessionPool, clientSessionHelper,
                mongoOperationPublisher.withTimeout(timeout, timeUnit));
    }

    public Cluster getCluster() {
        return cluster;
    }

    @Nullable
    public Crypt getCrypt() {
        return crypt;
    }

    public ClientSessionHelper getClientSessionHelper() {
        return clientSessionHelper;
    }

    public ServerSessionPool getServerSessionPool() {
        return serverSessionPool;
    }

    public MongoOperationPublisher<Document> getMongoOperationPublisher() {
        return mongoOperationPublisher;
    }

    public TimeoutSettings getTimeoutSettings() {
        return mongoOperationPublisher.getTimeoutSettings();
    }

    @Override
    public Publisher<ClientSession> startSession() {
        return startSession(ClientSessionOptions.builder().build());
    }

    @Override
    public Publisher<ClientSession> startSession(final ClientSessionOptions options) {
        notNull("options", options);
        return Mono.fromCallable(() -> clientSessionHelper.createClientSession(options, operationExecutor));
    }


    @Override
    public MongoDatabase getDatabase(final String name) {
        return new MongoDatabaseImpl(mongoOperationPublisher.withDatabase(name));
    }

    @Override
    public Publisher<String> listDatabaseNames() {
        return Flux.from(listDatabases().nameOnly(true)).map(d -> d.getString("name"));
    }

    @Override
    public Publisher<String> listDatabaseNames(final ClientSession clientSession) {
        return Flux.from(listDatabases(clientSession).nameOnly(true)).map(d -> d.getString("name"));
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public <T> ListDatabasesPublisher<T> listDatabases(final Class<T> clazz) {
        return new ListDatabasesPublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(clazz));
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <T> ListDatabasesPublisher<T> listDatabases(final ClientSession clientSession, final Class<T> clazz) {
        return new ListDatabasesPublisherImpl<>(notNull("clientSession", clientSession), mongoOperationPublisher.withDocumentClass(clazz));
    }

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return watch(Collections.emptyList());
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final Class<T> resultClass) {
        return watch(Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final List<? extends Bson> pipeline, final Class<T> resultClass) {
        return new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher.withDatabase("admin"),
                resultClass, pipeline, ChangeStreamLevel.CLIENT);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Collections.emptyList(), Document.class);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final ClientSession clientSession, final Class<T> resultClass) {
        return watch(clientSession, Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
            final Class<T> resultClass) {
        return new ChangeStreamPublisherImpl<>(notNull("clientSession", clientSession), mongoOperationPublisher.withDatabase("admin"),
                resultClass, pipeline, ChangeStreamLevel.CLIENT);
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(final List<? extends ClientNamespacedWriteModel> clientWriteModels) throws ClientBulkWriteException {
        notNull("clientWriteModels", clientWriteModels);
        isTrueArgument("`clientWriteModels` must not be empty", !clientWriteModels.isEmpty());
        return mongoOperationPublisher.clientBulkWrite(null, clientWriteModels, null);
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(final List<? extends ClientNamespacedWriteModel> clientWriteModels,
                                                      final ClientBulkWriteOptions options) throws ClientBulkWriteException {
        notNull("clientWriteModels", clientWriteModels);
        isTrueArgument("`clientWriteModels` must not be empty", !clientWriteModels.isEmpty());
        notNull("options", options);
        return mongoOperationPublisher.clientBulkWrite(null, clientWriteModels, options);
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(final ClientSession clientSession,
                                                      final List<? extends ClientNamespacedWriteModel> clientWriteModels) throws ClientBulkWriteException {
        notNull("clientSession", clientSession);
        notNull("clientWriteModels", clientWriteModels);
        isTrueArgument("`clientWriteModels` must not be empty", !clientWriteModels.isEmpty());
        return mongoOperationPublisher.clientBulkWrite(clientSession, clientWriteModels, null);
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(final ClientSession clientSession,
                                                      final List<? extends ClientNamespacedWriteModel> models,
                                                      final ClientBulkWriteOptions options) throws ClientBulkWriteException {
        return mongoOperationPublisher.clientBulkWrite(clientSession, models, options);
    }

}
