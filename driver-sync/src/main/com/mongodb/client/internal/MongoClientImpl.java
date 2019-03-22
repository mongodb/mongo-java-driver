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
import com.mongodb.ClientSessionOptions;
import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.internal.CodecRegistryHelper.createRegistry;
import static com.mongodb.client.internal.Crypts.createCrypt;
import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;

public final class MongoClientImpl implements MongoClient {

    private final MongoClientSettings settings;
    private final MongoClientDelegate delegate;

    public MongoClientImpl(final MongoClientSettings settings, @Nullable final MongoDriverInformation mongoDriverInformation) {
        this(createCluster(settings, mongoDriverInformation), settings, null);
    }

    public MongoClientImpl(final Cluster cluster, final MongoClientSettings settings,
                           @Nullable final OperationExecutor operationExecutor) {
        this.settings = notNull("settings", settings);
        AutoEncryptionSettings autoEncryptionSettings = settings.getAutoEncryptionSettings();
        this.delegate = new MongoClientDelegate(notNull("cluster", cluster),
                createRegistry(settings.getCodecRegistry(), settings.getUuidRepresentation()), this, operationExecutor,
                autoEncryptionSettings == null ? null : createCrypt(SimpleMongoClients.create(this), autoEncryptionSettings));
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return new MongoDatabaseImpl(databaseName, delegate.getCodecRegistry(), settings.getReadPreference(), settings.getWriteConcern(),
                settings.getRetryWrites(), settings.getRetryReads(), settings.getReadConcern(),
                settings.getUuidRepresentation(), delegate.getOperationExecutor());
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
    public ClientSession startSession() {
        return startSession(ClientSessionOptions
                .builder()
                .defaultTransactionOptions(TransactionOptions.builder()
                        .readConcern(settings.getReadConcern())
                        .writeConcern(settings.getWriteConcern())
                        .build())
                .build());
    }

    @Override
    public ClientSession startSession(final ClientSessionOptions options) {
        ClientSession clientSession = delegate.createClientSession(notNull("options", options),
                settings.getReadConcern(), settings.getWriteConcern(), settings.getReadPreference());
        if (clientSession == null) {
            throw new MongoClientException("Sessions are not supported by the MongoDB cluster to which this client is connected");
        }
        return clientSession;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return watch(Collections.<Bson>emptyList());
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.<Bson>emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createChangeStreamIterable(null, pipeline, resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Collections.<Bson>emptyList(), Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.<Bson>emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                         final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createChangeStreamIterable(clientSession, pipeline, resultClass);
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return delegate.getCluster().getCurrentDescription();
    }

    private <TResult> ChangeStreamIterable<TResult> createChangeStreamIterable(@Nullable final ClientSession clientSession,
                                                                               final List<? extends Bson> pipeline,
                                                                               final Class<TResult> resultClass) {
        return MongoIterables.changeStreamOf(clientSession, "admin", settings.getCodecRegistry(),
                settings.getReadPreference(), settings.getReadConcern(), delegate.getOperationExecutor(), pipeline, resultClass,
                ChangeStreamLevel.CLIENT, settings.getRetryReads());
    }

    public Cluster getCluster() {
        return delegate.getCluster();
    }

    public CodecRegistry getCodecRegistry() {
        return delegate.getCodecRegistry();
    }

    private static Cluster createCluster(final MongoClientSettings settings,
                                         @Nullable final MongoDriverInformation mongoDriverInformation) {
        notNull("settings", settings);
        return new DefaultClusterFactory().createCluster(settings.getClusterSettings(), settings.getServerSettings(),
                settings.getConnectionPoolSettings(), getStreamFactory(settings, false), getStreamFactory(settings, true),
                settings.getCredential(), getCommandListener(settings.getCommandListeners()), settings.getApplicationName(),
                mongoDriverInformation, settings.getCompressorList());
    }

    private static StreamFactory getStreamFactory(final MongoClientSettings settings, final boolean isHeartbeat) {
        StreamFactoryFactory streamFactoryFactory = settings.getStreamFactoryFactory();
        SocketSettings socketSettings = isHeartbeat ? settings.getHeartbeatSocketSettings() : settings.getSocketSettings();
        if (streamFactoryFactory == null) {
            return new SocketStreamFactory(socketSettings, settings.getSslSettings());
        } else {
            return streamFactoryFactory.create(socketSettings, settings.getSslSettings());
        }
    }

    private <T> ListDatabasesIterable<T> createListDatabasesIterable(@Nullable final ClientSession clientSession, final Class<T> clazz) {
        return MongoIterables.listDatabasesOf(clientSession, clazz, delegate.getCodecRegistry(),
                ReadPreference.primary(), delegate.getOperationExecutor(), settings.getRetryReads());
    }

    private MongoIterable<String> createListDatabaseNamesIterable(final @Nullable ClientSession clientSession) {
        return createListDatabasesIterable(clientSession, BsonDocument.class).nameOnly(true).map(new Function<BsonDocument, String>() {
            @Override
            public String apply(final BsonDocument result) {
                return result.getString("name").getValue();
            }
        });
    }
}

