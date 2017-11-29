/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.async.client;

import com.mongodb.ClientSessionOptions;
import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.Cluster;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.session.ClientSession;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.Closeable;
import java.io.IOException;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;

class MongoClientImpl implements MongoClient {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Cluster cluster;
    private final MongoClientSettings settings;
    private final AsyncOperationExecutor executor;
    private final Closeable externalResourceCloser;
    private final ServerSessionPool serverSessionPool;
    private final ClientSessionHelper clientSessionHelper;


    MongoClientImpl(final MongoClientSettings settings, final Cluster cluster, final Closeable externalResourceCloser) {
        this(settings, cluster, null, externalResourceCloser);
    }

    MongoClientImpl(final MongoClientSettings settings, final Cluster cluster, final AsyncOperationExecutor executor) {
        this(settings, cluster, executor, null);
    }

    private MongoClientImpl(final MongoClientSettings settings, final Cluster cluster, final AsyncOperationExecutor executor,
                            final Closeable externalResourceCloser) {
        this.settings = notNull("settings", settings);
        this.cluster = notNull("cluster", cluster);
        this.serverSessionPool = new ServerSessionPool(cluster);
        this.clientSessionHelper = new ClientSessionHelper(this, serverSessionPool);
        if (executor == null) {
            this.executor = new AsyncOperationExecutorImpl(this, clientSessionHelper);
        } else {
            this.executor = executor;
        }
        this.externalResourceCloser = externalResourceCloser;
    }

    @Override
    public void startSession(final ClientSessionOptions options, final SingleResultCallback<ClientSession> callback) {
        notNull("callback", callback);
        clientSessionHelper.createClientSession(notNull("options", options), new SingleResultCallback<ClientSession>() {
            @Override
            public void onResult(final ClientSession clientSession, final Throwable t) {
                SingleResultCallback<ClientSession> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else if (clientSession == null) {
                    errHandlingCallback.onResult(null, new MongoClientException("Sessions are not supported by the MongoDB cluster to"
                            + " which this client is connected"));
                } else {
                    errHandlingCallback.onResult(clientSession, null);
                }
            }
        });
    }

    @Override
    public MongoDatabase getDatabase(final String name) {
        return new MongoDatabaseImpl(name, settings.getCodecRegistry(), settings.getReadPreference(), settings.getWriteConcern(),
                settings.getRetryWrites(), settings.getReadConcern(), executor);
    }

    @Override
    public void close() {
        serverSessionPool.close();
        cluster.close();
        if (externalResourceCloser != null) {
            try {
                externalResourceCloser.close();
            } catch (IOException e) {
                LOGGER.warn("Exception closing resource", e);
            }
        }
    }

    @Override
    public MongoClientSettings getSettings() {
        return settings;
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

    private MongoIterable<String> createListDatabaseNamesIterable(final ClientSession clientSession) {
        return createListDatabasesIterable(clientSession, BsonDocument.class).nameOnly(true).map(new Function<BsonDocument, String>() {
            @Override
            public String apply(final BsonDocument result) {
                return result.getString("name").getValue();
            }
        });
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return createListDatabasesIterable(null, Document.class);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <T> ListDatabasesIterable<T> listDatabases(final Class<T> resultClass) {
        return createListDatabasesIterable(null, resultClass);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final ClientSession clientSession, final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createListDatabasesIterable(clientSession, resultClass);
    }

    private <T> ListDatabasesIterable<T> createListDatabasesIterable(final ClientSession clientSession, final Class<T> clazz) {
        return new ListDatabasesIterableImpl<T>(clientSession, clazz, settings.getCodecRegistry(),
                ReadPreference.primary(), executor);
    }

    Cluster getCluster() {
        return cluster;
    }

}
