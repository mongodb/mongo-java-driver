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

package com.mongodb.connection;

import com.mongodb.ServerAddress;
import com.mongodb.annotations.Immutable;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A client-generated identifier that uniquely identifies a MongoDB server.
 *
 * @since 3.0
 */
@Immutable
public final class ServerId {
    private final ClusterId clusterId;
    private final ServerAddress address;

    /**
     * Construct an instance.
     *
     * @param clusterId the client-generated cluster identifier
     * @param address the server address
     */
    public ServerId(final ClusterId clusterId, final ServerAddress address) {
        this.clusterId = notNull("clusterId", clusterId);
        this.address = notNull("address", address);
    }

    /**
     * Gets the cluster identifier.
     *
     * @return the cluster identifier
     */
    public ClusterId getClusterId() {
        return clusterId;
    }

    /**
     * Gets the server address.
     * @return the server address
     */
    public ServerAddress getAddress() {
        return address;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServerId serverId = (ServerId) o;

        if (!address.equals(serverId.address)) {
            return false;
        }
        if (!clusterId.equals(serverId.clusterId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = clusterId.hashCode();
        result = 31 * result + address.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ServerId{"
               + "clusterId=" + clusterId
               + ", address=" + address
               + '}';
    }
}
