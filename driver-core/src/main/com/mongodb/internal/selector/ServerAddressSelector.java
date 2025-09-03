/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.selector;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.selector.ServerSelector;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getByServerAddress;
import static java.util.Collections.singletonList;

/**
 * A server selector that chooses a server that matches the server address.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ServerAddressSelector implements ServerSelector {
    private final ServerAddress serverAddress;

    /**
     * Constructs a new instance.
     *
     * @param serverAddress the server address
     */
    public ServerAddressSelector(final ServerAddress serverAddress) {
        this.serverAddress = notNull("serverAddress", serverAddress);
    }

    /**
     * Gets the server address.
     *
     * @return the server address
     */
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public List<ServerDescription> select(final ClusterDescription clusterDescription) {
        ServerDescription serverDescription = getByServerAddress(clusterDescription, serverAddress);
        if (serverDescription != null) {
            return singletonList(serverDescription);
        }
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return "ServerAddressSelector{"
               + "serverAddress=" + serverAddress
               + '}';
    }
}
