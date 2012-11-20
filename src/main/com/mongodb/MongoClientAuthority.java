/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

import java.util.ArrayList;
import java.util.List;

//TODO: Add a constructor that takes a MongoURI or MongoClientURI
public class MongoClientAuthority {
    private final ServerAddress serverAddress;
    private final List<ServerAddress> serverAddresses;
    private final MongoClientCredentials credentials;

    public MongoClientAuthority(final ServerAddress serverAddress) {
        this(serverAddress, null);
    }

    public MongoClientAuthority(final ServerAddress serverAddress, MongoClientCredentials credentials) {
        this.serverAddress = serverAddress;
        this.credentials = credentials;
        this.serverAddresses = null;
    }

    public MongoClientAuthority(final List<ServerAddress> serverAddresses) {
        this(serverAddresses, null);
    }

    public MongoClientAuthority(final List<ServerAddress> serverAddresses, MongoClientCredentials credentials) {
        this.serverAddresses = serverAddresses;
        this.credentials = credentials;
        this.serverAddress = null;
    }

    public boolean isDirect() {
        return serverAddress != null;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public List<ServerAddress> getServerAddresses() {
        return new ArrayList<ServerAddress>(serverAddresses);
    }

    public MongoClientCredentials getCredentials() {
        return credentials;
    }
}
