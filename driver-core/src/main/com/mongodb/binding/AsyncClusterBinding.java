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

package com.mongodb.binding;

import com.mongodb.ReadPreference;
import com.mongodb.async.MongoFuture;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.Connection;
import com.mongodb.connection.Server;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.connection.ServerDescription;
import com.mongodb.selector.PrimaryServerSelector;
import com.mongodb.selector.ReadPreferenceServerSelector;
import com.mongodb.selector.ServerSelector;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A simple ReadWriteBinding implementation that supplies write connection sources bound to a possibly different primary each time and a
 * read connection source bound to a possible different server each time.
 *
 * @since 3.0
 */
public class AsyncClusterBinding extends AbstractReferenceCounted implements AsyncReadWriteBinding {
    private final Cluster cluster;
    private final ReadPreference readPreference;
    private final long maxWaitTimeMS;

    public AsyncClusterBinding(final Cluster cluster, final ReadPreference readPreference,
                               final long maxWaitTime, final TimeUnit timeUnit) {
        this.cluster = notNull("cluster", cluster);
        this.readPreference = notNull("readPreference", readPreference);
        this.maxWaitTimeMS = MILLISECONDS.convert(maxWaitTime, timeUnit);
    }

    @Override
    public AsyncReadWriteBinding retain() {
        super.retain();
        return this;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public MongoFuture<AsyncConnectionSource> getReadConnectionSource() {
        return new SingleResultFuture<AsyncConnectionSource>(new MyConnectionSource(new ReadPreferenceServerSelector(readPreference)));
    }

    @Override
    public MongoFuture<AsyncConnectionSource> getWriteConnectionSource() {
        return new SingleResultFuture<AsyncConnectionSource>(new MyConnectionSource(new PrimaryServerSelector()));
    }

    private final class MyConnectionSource extends AbstractReferenceCounted implements AsyncConnectionSource {
        private final Server server;

        private MyConnectionSource(final ServerSelector serverSelector) {
            this.server = cluster.selectServer(serverSelector, maxWaitTimeMS, MILLISECONDS);
            AsyncClusterBinding.this.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public MongoFuture<Connection> getConnection() {
            return new SingleResultFuture<Connection>(server.getConnection());
        }

        public AsyncConnectionSource retain() {
            super.retain();
            AsyncClusterBinding.this.retain();
            return this;
        }

        @Override
        public void release() {
            super.release();
            AsyncClusterBinding.this.release();
        }
    }
}
