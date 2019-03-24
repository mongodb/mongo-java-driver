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

package com.mongodb.embedded.client;

import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerDescription;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.embedded.capi.MongoEmbeddedInstance;
import com.mongodb.embedded.capi.MongoEmbeddedLibrary;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.ClusterClock;
import com.mongodb.internal.connection.ClusterClockAdvancingSessionContext;
import com.mongodb.internal.connection.CommandHelper;
import com.mongodb.internal.connection.CommandProtocol;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.DefaultServerConnection;
import com.mongodb.internal.connection.DescriptionHelper;
import com.mongodb.internal.connection.InternalConnection;
import com.mongodb.internal.connection.LegacyProtocol;
import com.mongodb.internal.connection.ProtocolExecutor;
import com.mongodb.internal.connection.Server;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.io.Closeable;
import java.io.File;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.connection.ClientMetadataHelper.createClientMetadataDocument;
import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;
import static java.lang.String.format;

@SuppressWarnings("deprecation")
class EmbeddedServer implements Server, Closeable {
    private static final Logger LOGGER = Loggers.getLogger("embedded.client");
    private static final MongoDriverInformation MONGO_DRIVER_INFORMATION = MongoDriverInformation.builder().driverName("embedded").build();
    private final MongoEmbeddedInstance instance;
    private final ClusterClock clusterClock;
    private final CommandListener commandListener;
    private final ServerAddress serverAddress;
    private final ServerDescription serverDescription;
    private final EmbeddedInternalConnectionPool connectionPool;

    private volatile boolean isClosed;


    EmbeddedServer(final MongoEmbeddedLibrary mongoEmbeddedLibrary, final MongoClientSettings mongoClientSettings) {
        this.instance = createInstance(mongoEmbeddedLibrary, mongoClientSettings);
        this.clusterClock = new ClusterClock();
        this.commandListener =  getCommandListener(mongoClientSettings.getCommandListeners());
        this.serverAddress = new ServerAddress();
        this.connectionPool = new EmbeddedInternalConnectionPool(new EmbeddedInternalConnectionFactory() {
            @Override
            public EmbeddedInternalConnection create() {
                return new EmbeddedInternalConnection(instance, commandListener,
                        createClientMetadataDocument(mongoClientSettings.getApplicationName(), MONGO_DRIVER_INFORMATION));
            }
        });

        this.serverDescription = createServerDescription();
    }

    @Override
    public ServerDescription getDescription() {
        isTrue("open", !isClosed);
        return serverDescription;
    }

    @Override
    public Connection getConnection() {
        isTrue("open", !isClosed);
        return new DefaultServerConnection(connectionPool.get(), new DefaultServerProtocolExecutor(), ClusterConnectionMode.SINGLE);
    }

    @Override
    public void getConnectionAsync(final SingleResultCallback<AsyncConnection> callback) {
        throw new UnsupportedOperationException("Async not supported");
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            connectionPool.close();
            instance.close();
        }
    }

    private MongoEmbeddedInstance createInstance(final MongoEmbeddedLibrary mongoEmbeddedLibrary,
                                                 final MongoClientSettings mongoClientSettings) {
        File directory = new File(mongoClientSettings.getDbPath());
        try {
            if (directory.mkdirs() && LOGGER.isInfoEnabled()) {
                LOGGER.info(format("Created dbpath directory: %s", mongoClientSettings.getDbPath()));
            }
        } catch (SecurityException e) {
            throw new MongoException(format("Could not validate / create the dbpath: %s", mongoClientSettings.getDbPath()));
        }

        String yamlConfig = createYamlConfig(mongoClientSettings);
        return mongoEmbeddedLibrary.createInstance(yamlConfig);
    }

    private String createYamlConfig(final MongoClientSettings mongoClientSettings) {
        return format("{ storage: { dbPath: %s } }", mongoClientSettings.getDbPath());
    }

    private class DefaultServerProtocolExecutor implements ProtocolExecutor {

        @Override
        public <T> T execute(final LegacyProtocol<T> protocol, final InternalConnection connection) {
            protocol.setCommandListener(commandListener);
            return protocol.execute(connection);
        }

        @Override
        public <T> void executeAsync(final LegacyProtocol<T> protocol, final InternalConnection connection,
                                     final SingleResultCallback<T> callback) {
            protocol.setCommandListener(commandListener);
            protocol.executeAsync(connection, callback);
        }

        @Override
        public <T> T execute(final CommandProtocol<T> protocol, final InternalConnection connection,
                             final SessionContext sessionContext) {
            protocol.sessionContext(new ClusterClockAdvancingSessionContext(sessionContext, clusterClock));
            return protocol.execute(connection);
        }

        @Override
        public <T> void executeAsync(final CommandProtocol<T> protocol, final InternalConnection connection,
                                     final SessionContext sessionContext, final SingleResultCallback<T> callback) {
            protocol.sessionContext(new ClusterClockAdvancingSessionContext(sessionContext, clusterClock));
            protocol.executeAsync(connection, errorHandlingCallback(callback, LOGGER));
        }
    }

    private ServerDescription createServerDescription() {
        InternalConnection connection = connectionPool.get();
        try {
            long start = System.nanoTime();
            BsonDocument isMasterResult = CommandHelper.executeCommand("admin", new BsonDocument("ismaster", new BsonInt32(1)),
                    clusterClock, connection);
            return DescriptionHelper.createServerDescription(serverAddress, isMasterResult, System.nanoTime() - start);
        } finally {
            connection.close();
        }
    }

}
