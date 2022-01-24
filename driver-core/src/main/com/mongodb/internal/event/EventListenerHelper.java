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

package com.mongodb.internal.event;

import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;

import java.util.List;

public final class EventListenerHelper {

    public static ClusterListener getClusterListener(final ClusterSettings clusterSettings) {
        switch (clusterSettings.getClusterListeners().size()) {
            case 0:
                return NO_OP_CLUSTER_LISTENER;
            case 1:
                return clusterSettings.getClusterListeners().get(0);
            default:
                return new ClusterListenerMulticaster(clusterSettings.getClusterListeners());
        }
    }

    public static CommandListener getCommandListener(final List<CommandListener> commandListeners) {
        switch (commandListeners.size()) {
            case 0:
                return null;
            case 1:
                return commandListeners.get(0);
            default:
                return new CommandListenerMulticaster(commandListeners);
        }
    }

    public static ConnectionPoolListener getConnectionPoolListener(final ConnectionPoolSettings connectionPoolSettings) {
        switch (connectionPoolSettings.getConnectionPoolListeners().size()) {
            case 0:
                return NO_OP_CONNECTION_POOL_LISTENER;
            case 1:
                return connectionPoolSettings.getConnectionPoolListeners().get(0);
            default:
                return new ConnectionPoolListenerMulticaster(connectionPoolSettings.getConnectionPoolListeners());
        }
    }

    public static ServerMonitorListener getServerMonitorListener(final ServerSettings serverSettings) {
        switch (serverSettings.getServerMonitorListeners().size()) {
            case 0:
                return NO_OP_SERVER_MONITOR_LISTENER;
            case 1:
                return serverSettings.getServerMonitorListeners().get(0);
            default:
                return new ServerMonitorListenerMulticaster(serverSettings.getServerMonitorListeners());
        }
    }

    public static ServerListener getServerListener(final ServerSettings serverSettings) {
        switch (serverSettings.getServerListeners().size()) {
            case 0:
                return NO_OP_SERVER_LISTENER;
            case 1:
                return serverSettings.getServerListeners().get(0);
            default:
                return new ServerListenerMulticaster(serverSettings.getServerListeners());
        }
    }

    public static final ServerListener NO_OP_SERVER_LISTENER = new ServerListener() {
    };

    public static final ServerMonitorListener NO_OP_SERVER_MONITOR_LISTENER = new ServerMonitorListener() {
    };

    public static final ClusterListener NO_OP_CLUSTER_LISTENER = new ClusterListener() {
    };

    private static final ConnectionPoolListener NO_OP_CONNECTION_POOL_LISTENER = new ConnectionPoolListener() {
    };

    private EventListenerHelper() {
    }
}
