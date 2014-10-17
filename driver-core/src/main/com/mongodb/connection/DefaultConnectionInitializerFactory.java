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

package com.mongodb.connection;

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

class DefaultConnectionInitializerFactory implements ConnectionInitializerFactory {
    private final ServerAddress serverAddress;
    private final List<MongoCredential> credentialList;

    DefaultConnectionInitializerFactory(final ServerAddress serverAddress, final List<MongoCredential> credentialList) {
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.credentialList = notNull("credentialList", credentialList);
    }

    @Override
    public ConnectionInitializer create(final InternalConnection connection) {
        return new DefaultConnectionInitializer(serverAddress, credentialList, connection);
    }

}
