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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ServerAddressSelector implements ServerSelector {
    private final ServerAddress serverAddress;

    public ServerAddressSelector(final ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public List<ServerDescription> choose(final ClusterDescription clusterDescription) {
        if (clusterDescription.getByServerAddress(serverAddress) != null) {
            return Arrays.asList(clusterDescription.getByServerAddress(serverAddress));
        }
        return Collections.emptyList();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServerAddressSelector that = (ServerAddressSelector) o;

        if (!serverAddress.equals(that.serverAddress)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return serverAddress.hashCode();
    }

    @Override
    public String toString() {
        return "ServerAddressSelector{"
               + "serverAddress=" + serverAddress
               + '}';
    }
}
