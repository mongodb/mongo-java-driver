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

import org.mongodb.connection.ConnectionFactory;
import org.mongodb.connection.ConnectionProvider;
import org.mongodb.connection.ConnectionProviderFactory;
import org.mongodb.connection.ServerAddress;

public class DefaultConnectionProviderFactory implements ConnectionProviderFactory {
    private final ConnectionProviderSettings settings;
    private final ConnectionFactory connectionFactory;

    public DefaultConnectionProviderFactory(final ConnectionProviderSettings settings, final ConnectionFactory connectionFactory) {
        this.settings = settings;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public ConnectionProvider create(final ServerAddress serverAddress) {
        return new DefaultConnectionProvider(serverAddress, connectionFactory, settings);
    }
}
