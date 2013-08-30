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
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.StreamFactory;

import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;

public class DefaultConnectionFactory implements ConnectionFactory {
    private StreamFactory streamFactory;
    private BufferProvider bufferProvider;
    private List<MongoCredential> credentialList;

    public DefaultConnectionFactory(final StreamFactory streamFactory, final BufferProvider bufferProvider,
                                    final List<MongoCredential> credentialList) {
        this.streamFactory = streamFactory;
        this.bufferProvider = notNull("bufferProvider", bufferProvider);
        this.credentialList = notNull("credentialList", credentialList);
    }

    @Override
    public Connection create(final ServerAddress serverAddress) {
        return new DefaultConnection(streamFactory.create(serverAddress), credentialList, bufferProvider);
    }
}