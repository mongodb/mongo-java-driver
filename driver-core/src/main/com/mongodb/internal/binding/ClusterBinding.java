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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.connection.ServerTuple;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.selector.ReadPreferenceWithFallbackServerSelector;
import com.mongodb.internal.selector.ServerAddressSelector;
import com.mongodb.internal.selector.WritableServerSelector;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A simple ReadWriteBinding implementation that supplies write connection sources bound to a possibly different primary each time, and a
 * read connection source bound to a possible different server each time.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ClusterBinding extends AbstractReferenceCounted implements ClusterAwareReadWriteBinding {
    private final Cluster cluster;
    private final ReadPreference readPreference;

    /**
     * Creates an instance.
     * @param cluster          a non-null Cluster which will be used to select a server to bind to
     * @param readPreference   a non-null ReadPreference for read operations
     */
    public ClusterBinding(final Cluster cluster, final ReadPreference readPreference) {
        this.cluster = notNull("cluster", cluster);
        this.readPreference = notNull("readPreference", readPreference);
    }

    @Override
    public ReadWriteBinding retain() {
        super.retain();
        return this;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public ConnectionSource getReadConnectionSource(final OperationContext operationContext) {
        return new ClusterBindingConnectionSource(
                cluster.selectServer(new ReadPreferenceServerSelector(readPreference), operationContext),
                readPreference);
    }

    @Override
    public ConnectionSource getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference,
                                                    final OperationContext operationContext) {
        // Assume 5.0+ for load-balanced mode
        if (cluster.getSettings().getMode() == ClusterConnectionMode.LOAD_BALANCED) {
            return getReadConnectionSource(operationContext);
        } else {
            ReadPreferenceWithFallbackServerSelector readPreferenceWithFallbackServerSelector
                    = new ReadPreferenceWithFallbackServerSelector(readPreference, minWireVersion, fallbackReadPreference);
            ServerTuple serverTuple = cluster.selectServer(readPreferenceWithFallbackServerSelector, operationContext);
            return new ClusterBindingConnectionSource(serverTuple,
                    readPreferenceWithFallbackServerSelector.getAppliedReadPreference());
        }
    }

    @Override
    public ConnectionSource getWriteConnectionSource(final OperationContext operationContext) {
        return new ClusterBindingConnectionSource(
                cluster.selectServer(new WritableServerSelector(), operationContext),
                readPreference);
    }

    @Override
    public ConnectionSource getConnectionSource(final ServerAddress serverAddress, final OperationContext operationContext) {
        return new ClusterBindingConnectionSource(
                cluster.selectServer(new ServerAddressSelector(serverAddress), operationContext),
                readPreference);
    }

    private final class ClusterBindingConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private final Server server;
        private final ServerDescription serverDescription;
        private final ReadPreference appliedReadPreference;

        private ClusterBindingConnectionSource(final ServerTuple serverTuple, final ReadPreference appliedReadPreference) {
            this.server = serverTuple.getServer();
            this.serverDescription = serverTuple.getServerDescription();
            this.appliedReadPreference = appliedReadPreference;
            ClusterBinding.this.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            return serverDescription;
        }

        @Override
        public ReadPreference getReadPreference() {
            return appliedReadPreference;
        }

        @Override
        public Connection getConnection(final OperationContext operationContext) {
            /*
             The first read in a causally consistent session MUST not send afterClusterTime to the server
             (because the operationTime has not yet been determined). Therefore, we use ReadConcernAwareNoOpSessionContext
             so that we do not advance clusterTime on ClientSession in given operationContext because it might not be yet set.
             */
            ReadConcern readConcern = operationContext.getSessionContext().getReadConcern();
            return server.getConnection(operationContext.withSessionContext(new ReadConcernAwareNoOpSessionContext(readConcern)));
        }

        public ConnectionSource retain() {
            super.retain();
            ClusterBinding.this.retain();
            return this;
        }

        @Override
        public int release() {
            int count = super.release();
            ClusterBinding.this.release();
            return count;
        }
    }
}
