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

package org.mongodb.connection.impl;

import org.mongodb.MongoCredential;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ConnectionFactory;
import org.mongodb.connection.SSLSettings;
import org.mongodb.connection.ServerAddress;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;

public class DefaultConnectionFactory implements ConnectionFactory {
    private final DefaultConnectionSettings settings;
    private final SSLSettings sslSettings;
    private BufferProvider bufferProvider;
    private List<MongoCredential> credentialList;

    public DefaultConnectionFactory(final DefaultConnectionSettings settings,
                                    final SSLSettings sslSettings, final BufferProvider bufferProvider,
                                    final List<MongoCredential> credentialList) {
        this.settings = notNull("settings", settings);
        this.sslSettings = notNull("sslSettings", sslSettings);
        this.bufferProvider = notNull("bufferProvider", bufferProvider);
        this.credentialList = notNull("credentialList", credentialList);
    }

    @Override
    public Connection create(final ServerAddress serverAddress) {
        Connection socketConnection;
        if (sslSettings.isEnabled()) {
            socketConnection = new DefaultSocketConnection(serverAddress, settings, bufferProvider, SSLSocketFactory.getDefault());
        }
        else if (System.getProperty("org.mongodb.useSocket", "false").equals("true")) {
            socketConnection = new DefaultSocketConnection(serverAddress, settings, bufferProvider, SocketFactory.getDefault());
        }
        else {
            socketConnection = new DefaultSocketChannelConnection(serverAddress, settings, bufferProvider);
        }
        return new AuthenticatingConnection(socketConnection, credentialList, bufferProvider);
    }
}