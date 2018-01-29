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

package com.mongodb.connection;

import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerType.UNKNOWN;

class TestServerMonitor implements ServerMonitor {
    private ServerDescription currentDescription;
    private ChangeListener<ServerDescription> serverStateListener;

    TestServerMonitor(final ServerId serverId) {
        currentDescription = ServerDescription.builder().type(UNKNOWN).state(CONNECTING).address(serverId.getAddress()).build();
    }

    @Override
    public void start() {
    }

    @Override
    public void connect() {
    }

    @Override
    public void close() {
    }

    public void setServerStateListener(final ChangeListener<ServerDescription> serverStateListener) {
        this.serverStateListener = serverStateListener;
    }


    public void sendNotification(final ServerDescription serverDescription) {
        serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(currentDescription, serverDescription));
        currentDescription = serverDescription;
    }
}
