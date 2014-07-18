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
import com.mongodb.ServerAddress;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.Connection;
import com.mongodb.selector.ServerAddressSelector;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A simple binding where all connection sources are bound to the server specified in the constructor.
 *
 * @since 3.0
 */
public class SingleServerBinding extends AbstractReferenceCounted implements ReadWriteBinding {
    private final Cluster cluster;
    private final ServerAddress serverAddress;
    private final ReadPreference readPreference;
    private long maxWaitTimeMS;

    public SingleServerBinding(final Cluster cluster, final ServerAddress serverAddress, final long maxWaitTime, final TimeUnit timeUnit) {
        this(cluster, serverAddress, ReadPreference.primary(), maxWaitTime, timeUnit);
    }

    public SingleServerBinding(final Cluster cluster, final ServerAddress serverAddress, final ReadPreference readPreference,
                               final long maxWaitTime, final TimeUnit timeUnit) {
        this.cluster = cluster;
        this.serverAddress = serverAddress;
        this.readPreference = readPreference;
        this.maxWaitTimeMS = MILLISECONDS.convert(maxWaitTime, timeUnit);
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        return new MyConnectionSource();
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        return new MyConnectionSource();
    }

    @Override
    public SingleServerBinding retain() {
        super.retain();
        return this;
    }

    private final class MyConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private MyConnectionSource() {
            SingleServerBinding.this.retain();
        }

        @Override
        public Connection getConnection() {
            return cluster.selectServer(new ServerAddressSelector(serverAddress), maxWaitTimeMS, MILLISECONDS).getConnection();
        }

        @Override
        public ConnectionSource retain() {
            super.retain();
            SingleServerBinding.this.retain();
            return this;
        }

        @Override
        public void release() {
            SingleServerBinding.this.release();
        }
    }
}

