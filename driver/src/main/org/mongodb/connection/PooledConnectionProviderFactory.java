/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.mongodb.MongoCredential;
import org.mongodb.event.ConnectionListener;
import org.mongodb.event.ConnectionPoolListener;

import java.util.List;

class PooledConnectionProviderFactory implements ConnectionProviderFactory {
    private final String clusterId;
    private final ConnectionPoolSettings settings;
    private final StreamFactory streamFactory;
    private final List<MongoCredential> credentialList;
    private final ConnectionListener connectionListener;
    private final ConnectionPoolListener connectionPoolListener;

    public PooledConnectionProviderFactory(final String clusterId, final ConnectionPoolSettings settings, final StreamFactory streamFactory,
                                           final List<MongoCredential> credentialList,
                                           final ConnectionListener connectionListener,
                                           final ConnectionPoolListener connectionPoolListener) {
        this.clusterId = clusterId;
        this.settings = settings;
        this.streamFactory = streamFactory;
        this.credentialList = credentialList;
        this.connectionListener = connectionListener;
        this.connectionPoolListener = connectionPoolListener;
    }

    @Override
    public ConnectionProvider create(final ServerAddress serverAddress) {
        return new PooledConnectionProvider(clusterId, serverAddress,
                                            new InternalStreamConnectionFactory(clusterId,
                                                                                streamFactory,
                                                                                credentialList,
                                                                                connectionListener),
                                            settings,
                                            connectionPoolListener);
    }
}
