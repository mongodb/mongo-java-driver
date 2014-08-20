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
import com.mongodb.selector.ReadPreferenceServerSelector;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An asynchronous factory of providing a single connection source for servers that can be read from or written to.
 *
 * @since 3.0
 */
public class AsyncSingleConnectionBinding extends AbstractReferenceCounted implements AsyncReadWriteBinding {
    private final Server server;
    private Connection connection;

    public AsyncSingleConnectionBinding(final Cluster cluster, final long maxWaitTime, final TimeUnit timeUnit) {
        notNull("cluster", cluster);
        notNull("maxWaitTime", maxWaitTime);
        notNull("timeUnit", timeUnit);

        long maxWaitTimeMS = MILLISECONDS.convert(maxWaitTime, timeUnit);
        this.server =  cluster.selectServer(new ReadPreferenceServerSelector(getReadPreference()), maxWaitTimeMS, MILLISECONDS);
    }

    @Override
    public AsyncReadWriteBinding retain() {
        super.retain();
        return this;
    }

    @Override
    public ReadPreference getReadPreference() {
        return ReadPreference.primary();
    }

    @Override
    public MongoFuture<AsyncConnectionSource> getReadConnectionSource() {
        return getConnectionSource();
    }

    @Override
    public MongoFuture<AsyncConnectionSource> getWriteConnectionSource() {
        return getConnectionSource();
    }

    public void release() {
        super.release();
        if (getCount() == 0 && connection != null) {
            connection.release();
        }
    }

    private MongoFuture<AsyncConnectionSource> getConnectionSource() {
        isTrue("open", getCount() > 0);
        if (connection == null) {
            connection = server.getConnection();
        }
        return new SingleResultFuture<AsyncConnectionSource>(new MyConnectionSource(connection));
    }

    private static final class MyConnectionSource extends AbstractReferenceCounted implements AsyncConnectionSource {
        private final Connection connection;

        private MyConnectionSource(final Connection connection) {
            this.connection = connection.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            return connection.getServerDescription();
        }

        @Override
        public MongoFuture<Connection> getConnection() {
          isTrue("open", getCount() > 0);
          return new SingleResultFuture<Connection>(connection.retain());
        }

        public AsyncConnectionSource retain() {
            super.retain();
            return this;
        }

        @Override
        public void release() {
            super.release();
            if (getCount() == 0) {
                connection.release();
            }
        }
    }
}
