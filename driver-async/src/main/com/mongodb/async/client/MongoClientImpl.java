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

import com.mongodb.Function;
import com.mongodb.ReadPreference;
import com.mongodb.connection.Cluster;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.internal.session.ServerSessionPool;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.Closeable;
import java.io.IOException;

import static com.mongodb.assertions.Assertions.notNull;

class MongoClientImpl implements MongoClient {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Cluster cluster;
    private final MongoClientSettings settings;
    private final AsyncOperationExecutor executor;
    private final Closeable externalResourceCloser;
    private final ServerSessionPool serverSessionPool;

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
        if (executor == null) {
            this.executor = new AsyncOperationExecutorImpl(this);
        } else {
            this.executor = executor;
        }
        this.externalResourceCloser = externalResourceCloser;
        this.serverSessionPool = new ServerSessionPool(cluster);
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
        return new ListDatabasesIterableImpl<BsonDocument>(BsonDocument.class, MongoClients.getDefaultCodecRegistry(),
                                                           ReadPreference.primary(), executor).map(new Function<BsonDocument, String>() {
            @Override
            public String apply(final BsonDocument document) {
                return document.getString("name").getValue();
            }
        });
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public <T> ListDatabasesIterable<T> listDatabases(final Class<T> resultClass) {
        return new ListDatabasesIterableImpl<T>(resultClass, settings.getCodecRegistry(), ReadPreference.primary(), executor);
    }

    Cluster getCluster() {
        return cluster;
    }

    ServerSessionPool getServerSessionPool() {
        return serverSessionPool;
    }
}
