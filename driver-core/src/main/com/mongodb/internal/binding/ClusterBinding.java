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
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.selector.WritableServerSelector;
import com.mongodb.selector.ServerSelector;
import com.mongodb.session.SessionContext;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A simple ReadWriteBinding implementation that supplies write connection sources bound to a possibly different primary each time, and a
 * read connection source bound to a possible different server each time.
 *
 * @since 3.0
 */
public class ClusterBinding extends AbstractReferenceCounted implements ClusterAwareReadWriteBinding {
    private final Cluster cluster;
    private final ReadPreference readPreference;
    private final ReadConcern readConcern;

    /**
     * Creates an instance.
     * @param cluster        a non-null Cluster which will be used to select a server to bind to
     * @param readPreference a non-null ReadPreference for read operations
     * @param readConcern    a non-null read concern
     * @since 3.8
     */
    public ClusterBinding(final Cluster cluster, final ReadPreference readPreference, final ReadConcern readConcern) {
        this.cluster = notNull("cluster", cluster);
        this.readPreference = notNull("readPreference", readPreference);
        this.readConcern = notNull("readConcern", readConcern);
    }

    /**
     * Return the cluster.
     * @return the cluster
     * @since 3.11
     */
    public Cluster getCluster() {
        return cluster;
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
    public ConnectionSource getReadConnectionSource() {
        return new ClusterBindingConnectionSource(new ReadPreferenceServerSelector(readPreference));
    }

    @Override
    public SessionContext getSessionContext() {
        return new ReadConcernAwareNoOpSessionContext(readConcern);
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        return new ClusterBindingConnectionSource(new WritableServerSelector());
    }

    private final class ClusterBindingConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private final Server server;

        private ClusterBindingConnectionSource(final ServerSelector serverSelector) {
            this.server = cluster.selectServer(serverSelector);
            ClusterBinding.this.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public SessionContext getSessionContext() {
            return new ReadConcernAwareNoOpSessionContext(readConcern);
        }

        @Override
        public Connection getConnection() {
            return server.getConnection();
        }

        public ConnectionSource retain() {
            super.retain();
            ClusterBinding.this.retain();
            return this;
        }

        @Override
        public void release() {
            super.release();
            ClusterBinding.this.release();
        }
    }
}
