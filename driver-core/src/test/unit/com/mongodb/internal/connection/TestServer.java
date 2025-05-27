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

package com.mongodb.internal.connection;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.internal.async.SingleResultCallback;

import static com.mongodb.connection.ServerConnectionState.CONNECTING;

public class TestServer implements ClusterableServer {
    private final Cluster cluster;
    private final ServerListener serverListener;
    private ServerDescription description;
    private boolean isClosed;
    private final ServerId serverId;
    private int connectCount;

    public TestServer(final ServerAddress serverAddress, final Cluster cluster, final ServerListener serverListener) {
        this.serverId = new ServerId(new ClusterId(), serverAddress);
        this.cluster = cluster;
        this.serverListener = serverListener;
        this.description = ServerDescription.builder().state(CONNECTING).address(serverId.getAddress()).build();
        sendNotification(ServerDescription.builder().state(CONNECTING).address(serverId.getAddress()).build());
    }

    public void sendNotification(final ServerDescription newDescription) {
        ServerDescription currentDescription = description;
        description = newDescription;
        ServerDescriptionChangedEvent event = new ServerDescriptionChangedEvent(serverId, newDescription, currentDescription);
        if (cluster != null) {
            cluster.onChange(event);
        }
        if (serverListener != null) {
            serverListener.serverDescriptionChanged(event);
        }
    }

    @Override
    public void resetToConnecting(final MongoException cause) {
        sendNotification(ServerDescription.builder().state(CONNECTING).exception(cause).address(serverId.getAddress()).build());
    }

    @Override
    public void invalidate(final MongoException cause) {
        sendNotification(ServerDescription.builder().state(CONNECTING).exception(cause).address(serverId.getAddress()).build());
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void connect() {
        connectCount++;
    }

    public int getConnectCount() {
        return connectCount;
    }

    public ServerDescription getDescription() {
        return description;
    }

    @Override
    public Connection getConnection(final OperationContext operationContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getConnectionAsync(final OperationContext operationContext, final SingleResultCallback<AsyncConnection> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int operationCount() {
        return -1;
    }
}
