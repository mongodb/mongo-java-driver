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

import com.mongodb.MongoCompressor;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.Connection;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.Stream;
import com.mongodb.connection.StreamFactory;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.connection.Authenticator;
import com.mongodb.internal.connection.ClusterClock;
import com.mongodb.internal.connection.ClusterClockAdvancingSessionContext;
import com.mongodb.internal.connection.CommandHelper;
import com.mongodb.internal.connection.CommandProtocol;
import com.mongodb.internal.connection.DefaultServerConnection;
import com.mongodb.internal.connection.DescriptionHelper;
import com.mongodb.internal.connection.InternalConnection;
import com.mongodb.internal.connection.InternalStreamConnection;
import com.mongodb.internal.connection.InternalStreamConnectionInitializer;
import com.mongodb.internal.connection.LegacyProtocol;
import com.mongodb.internal.connection.ProtocolExecutor;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.io.Closeable;
import java.util.Collections;

import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.connection.ClientMetadataHelper.createClientMetadataDocument;
import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;

class EmbeddedServer implements Server, Closeable {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private static final MongoDriverInformation MONGO_DRIVER_INFORMATION = MongoDriverInformation.builder()
            .driverName("embedded").build();
    private final BsonDocument clientMetadataDocument;
    private final ClusterId clusterId;
    private final ClusterClock clusterClock;
    private final CommandListener commandListener;
    private final ServerAddress serverAddress;
    private final EmbeddedDatabase embeddedDatabase;
    private final ServerDescription serverDescription;

    EmbeddedServer(final MongoClientSettings mongoClientSettings) {
        this.embeddedDatabase = new EmbeddedDatabase(mongoClientSettings);
        this.clientMetadataDocument = createClientMetadataDocument(mongoClientSettings.getApplicationName(), MONGO_DRIVER_INFORMATION);
        this.clusterId = new ClusterId();
        this.clusterClock = new ClusterClock();
        this.commandListener =  getCommandListener(mongoClientSettings.getCommandListeners());
        this.serverAddress = new ServerAddress();
        this.serverDescription = createServerDescription();
    }

    @Override
    public ServerDescription getDescription() {
        return serverDescription;
    }

    @Override
    public Connection getConnection() {
        InternalConnection internalConnection = getInternalConnection();
        internalConnection.open();
        return new DefaultServerConnection(internalConnection, new DefaultServerProtocolExecutor(), ClusterConnectionMode.SINGLE);
    }

    @Override
    public void getConnectionAsync(final SingleResultCallback<AsyncConnection> callback) {
        throw new UnsupportedOperationException("Async not supported");
    }

    /**
     * Pump the message queue.
     */
    public void pump() {
        embeddedDatabase.pump();
    }

    @Override
    public void close() {
        embeddedDatabase.close();
    }

    private InternalConnection getInternalConnection() {
        return new InternalStreamConnection(new ServerId(clusterId, serverAddress),
                new StreamFactory() {
                    @Override
                    public Stream create(final ServerAddress serverAddress) {
                        return new EmbeddedStream(embeddedDatabase.createConnection());
                    }
                }, Collections.<MongoCompressor>emptyList(), commandListener,
                new InternalStreamConnectionInitializer(Collections.<Authenticator>emptyList(), clientMetadataDocument,
                        Collections.<MongoCompressor>emptyList()));
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
        InternalConnection connection = getInternalConnection();
        try {
            connection.open();
            long start = System.nanoTime();
            BsonDocument isMasterResult = CommandHelper.executeCommand("admin", new BsonDocument("ismaster", new BsonInt32(1)),
                    clusterClock, connection);
            return DescriptionHelper.createServerDescription(serverAddress, isMasterResult, new ServerVersion(), System.nanoTime() - start);
        } finally {
            connection.close();
        }
    }

}
