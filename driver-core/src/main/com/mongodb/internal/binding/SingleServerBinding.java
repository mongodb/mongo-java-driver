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

package com.mongodb.internal.binding;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.ServerTuple;
import com.mongodb.internal.selector.ServerAddressSelector;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A simple binding where all connection sources are bound to the server specified in the constructor.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class SingleServerBinding extends AbstractReferenceCounted implements ReadWriteBinding {
    private final Cluster cluster;
    private final ServerAddress serverAddress;

    /**
     * Creates an instance, defaulting to {@link com.mongodb.ReadPreference#primary()} for reads.
     * @param cluster       a non-null  Cluster which will be used to select a server to bind to
     * @param serverAddress a non-null  address of the server to bind to
     */
    public SingleServerBinding(final Cluster cluster, final ServerAddress serverAddress) {
        this.cluster = notNull("cluster", cluster);
        this.serverAddress = notNull("serverAddress", serverAddress);
    }

    @Override
    public ConnectionSource getWriteConnectionSource(final OperationContext operationContext) {
        ServerTuple serverTuple = cluster.selectServer(new ServerAddressSelector(serverAddress), operationContext);
        return new SingleServerBindingConnectionSource(serverTuple);
    }

    @Override
    public ReadPreference getReadPreference() {
        return ReadPreference.primary();
    }

    @Override
    public ConnectionSource getReadConnectionSource(final OperationContext operationContext) {
        ServerTuple serverTuple = cluster.selectServer(new ServerAddressSelector(serverAddress), operationContext);
        return new SingleServerBindingConnectionSource(serverTuple);
    }

    @Override
    public ConnectionSource getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference,
                                                    final OperationContext operationContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SingleServerBinding retain() {
        super.retain();
        return this;
    }

    private final class SingleServerBindingConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private final ServerDescription serverDescription;

        private SingleServerBindingConnectionSource(final ServerTuple serverTuple) {
            SingleServerBinding.this.retain();
            this.serverDescription = serverTuple.getServerDescription();
        }

        @Override
        public ServerDescription getServerDescription() {
            return serverDescription;
        }

        @Override
        public ReadPreference getReadPreference() {
            return ReadPreference.primary();
        }

        @Override
        public Connection getConnection(final OperationContext operationContext) {
            return cluster
                    .selectServer(new ServerAddressSelector(serverAddress), operationContext)
                    .getServer()
                    .getConnection(operationContext);
        }

        @Override
        public ConnectionSource retain() {
            super.retain();
            return this;
        }

        @Override
        public int release() {
            int count = super.release();
            if (count == 0) {
                SingleServerBinding.this.release();
            }
            return count;
        }
    }
}
