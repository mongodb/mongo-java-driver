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

package com.mongodb.client;

import com.mongodb.ClientSessionOptions;
import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadPreference;
import com.mongodb.client.internal.ListDatabasesIterableImpl;
import com.mongodb.client.internal.MongoClientDelegate;
import com.mongodb.client.internal.MongoDatabaseImpl;
import com.mongodb.client.internal.OperationExecutor;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.StreamFactory;
import com.mongodb.session.ClientSession;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;

final class MongoClientImpl implements MongoClient {

    private final MongoClientSettings settings;
    private final MongoClientDelegate delegate;

    MongoClientImpl(final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation) {
        this(createCluster(settings, mongoDriverInformation), settings, null);
    }

    private MongoClientImpl(final Cluster cluster, final MongoClientSettings settings, final OperationExecutor operationExecutor) {
        this.settings = notNull("settings", settings);
        this.delegate = new MongoClientDelegate(notNull("cluster", cluster),
                Collections.singletonList(settings.getCredential()), this, operationExecutor);
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return new MongoDatabaseImpl(databaseName, settings.getCodecRegistry(), settings.getReadPreference(), settings.getWriteConcern(),
                settings.getRetryWrites(), settings.getReadConcern(), delegate.getOperationExecutor());
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
    public <T> ListDatabasesIterable<T> listDatabases(final Class<T> clazz) {
        return createListDatabasesIterable(null, clazz);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <T> ListDatabasesIterable<T> listDatabases(final ClientSession clientSession, final Class<T> clazz) {
        notNull("clientSession", clientSession);
        return createListDatabasesIterable(clientSession, clazz);
    }

    @Override
    public ClientSession startSession(final ClientSessionOptions options) {
        ClientSession clientSession = delegate.createClientSession(notNull("options", options));
        if (clientSession == null) {
            throw new MongoClientException("Sessions are not supported by the MongoDB cluster to which this client is connected");
        }
        return clientSession;
    }

    @Override
    public void close() {
        delegate.close();
    }

    Cluster getCluster() {
        return delegate.getCluster();
    }

    private static Cluster createCluster(final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation) {
        notNull("settings", settings);
        List<MongoCredential> credentialList = settings.getCredential() != null ? Collections.singletonList(settings.getCredential())
                : Collections.<MongoCredential>emptyList();
        return new DefaultClusterFactory().createCluster(settings.getClusterSettings(), settings.getServerSettings(),
                settings.getConnectionPoolSettings(), getStreamFactory(settings), getStreamFactory(settings), credentialList,
                getCommandListener(settings.getCommandListeners()), settings.getApplicationName(), mongoDriverInformation,
                settings.getCompressorList());
    }

    private static StreamFactory getStreamFactory(final MongoClientSettings settings) {
        if (settings.getStreamFactoryFactory() == null) {
            return new SocketStreamFactory(settings.getSocketSettings(), settings.getSslSettings());
        } else {
            return settings.getStreamFactoryFactory().create(settings.getSocketSettings(), settings.getSslSettings());
        }
    }

    private <T> ListDatabasesIterable<T> createListDatabasesIterable(final ClientSession clientSession, final Class<T> clazz) {
        return new ListDatabasesIterableImpl<T>(clientSession, clazz, settings.getCodecRegistry(),
                ReadPreference.primary(), delegate.getOperationExecutor());
    }

    private MongoIterable<String> createListDatabaseNamesIterable(final ClientSession clientSession) {
        return createListDatabasesIterable(clientSession, BsonDocument.class).nameOnly(true).map(new Function<BsonDocument, String>() {
            @Override
            public String apply(final BsonDocument result) {
                return result.getString("name").getValue();
            }
        });
    }
}

