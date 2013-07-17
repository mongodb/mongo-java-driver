/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import static org.mongodb.connection.ServerConnectionState.Connecting;

public class TestServer implements ClusterableServer {
    private ChangeListener<ServerDescription> changeListener;
    private ServerDescription description;
    private boolean isClosed;
    private boolean isInvalidated;

    public TestServer(final ServerAddress serverAddress) {
        description = ServerDescription.builder().state(Connecting).address(serverAddress).build();
    }

    public void sendNotification(final ServerDescription newDescription) {
        ServerDescription currentDescription = description;
        description = newDescription;
        changeListener.stateChanged(new ChangeEvent<ServerDescription>(currentDescription, newDescription));
    }

    public boolean isInvalidated() {
        return isInvalidated;
    }

    public void clearInvalidated() {
        isInvalidated = false;
    }

    @Override
    public void addChangeListener(final ChangeListener<ServerDescription> newChangeListener) {
        this.changeListener = newChangeListener;
    }

    @Override
    public void invalidate() {
        isInvalidated = true;
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
    public ServerDescription getDescription() {
        return description;
    }

    @Override
    public Connection getConnection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncServerConnection getAsyncConnection() {
        throw new UnsupportedOperationException();
    }
}
