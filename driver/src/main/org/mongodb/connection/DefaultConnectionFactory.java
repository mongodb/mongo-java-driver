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

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.nio.ByteBuffer;
import java.util.List;

class DefaultConnectionFactory implements ConnectionFactory {
    private MongoClientOptions options;
    private ServerAddress serverAddress;
    private BufferPool<ByteBuffer> bufferPool;
    private List<MongoCredential> credentialList;

    public DefaultConnectionFactory(final MongoClientOptions options, final ServerAddress serverAddress,
                                    final BufferPool<ByteBuffer> bufferPool, final List<MongoCredential> credentialList) {
        this.options = options;
        this.serverAddress = serverAddress;
        this.bufferPool = bufferPool;
        this.credentialList = credentialList;
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public Connection create() {
        Connection socketConnection;
        if (options.isSSLEnabled()) {
            socketConnection = new DefaultSocketConnection(serverAddress, bufferPool, SSLSocketFactory.getDefault());
        }
        else if (System.getProperty("org.mongodb.useSocket", "false").equals("true")) {
            socketConnection = new DefaultSocketConnection(serverAddress, bufferPool, SocketFactory.getDefault());
        }
        else {
            socketConnection = new DefaultSocketChannelConnection(serverAddress, bufferPool);
        }
        return new AuthenticatingConnection(socketConnection, credentialList, bufferPool);
    }
}