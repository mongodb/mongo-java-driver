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

import org.mongodb.connection.AsyncConnectionFactory;
import org.mongodb.connection.AsyncConnectionProvider;
import org.mongodb.connection.AsyncConnectionProviderFactory;
import org.mongodb.connection.ServerAddress;

public class DefaultAsyncConnectionProviderFactory implements AsyncConnectionProviderFactory {
    private final ConnectionProviderSettings settings;
    private final AsyncConnectionFactory connectionFactory;

    public DefaultAsyncConnectionProviderFactory(final ConnectionProviderSettings settings,
                                                 final AsyncConnectionFactory connectionFactory) {
        this.settings = settings;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public AsyncConnectionProvider create(final ServerAddress serverAddress) {
        return new DefaultAsyncConnectionProvider(serverAddress, connectionFactory, settings);
    }
}
