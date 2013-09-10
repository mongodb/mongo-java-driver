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

import org.mongodb.MongoCredential;
import org.mongodb.event.ConnectionListener;

import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;

class InternalStreamConnectionFactory implements InternalConnectionFactory {
    private StreamFactory streamFactory;
    private BufferProvider bufferProvider;
    private List<MongoCredential> credentialList;
    private ConnectionListener connectionListener;

    public InternalStreamConnectionFactory(final StreamFactory streamFactory, final BufferProvider bufferProvider,
                                           final List<MongoCredential> credentialList,
                                           final ConnectionListener connectionListener) {
        this.streamFactory = streamFactory;
        this.bufferProvider = notNull("bufferProvider", bufferProvider);
        this.credentialList = notNull("credentialList", credentialList);
        this.connectionListener = notNull("connectionListener", connectionListener);
    }

    @Override
    public InternalConnection create(final ServerAddress serverAddress) {
        return new InternalStreamConnection(streamFactory.create(serverAddress), credentialList, bufferProvider, connectionListener);
    }
}