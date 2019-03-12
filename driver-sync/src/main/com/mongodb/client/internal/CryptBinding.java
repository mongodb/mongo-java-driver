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

package com.mongodb.client.internal;

import com.mongodb.ReadPreference;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.ClusterAwareReadWriteBinding;
import com.mongodb.session.SessionContext;

class CryptBinding implements ClusterAwareReadWriteBinding {
    private final ClusterAwareReadWriteBinding wrapped;
    private final Crypt crypt;

    CryptBinding(final ClusterAwareReadWriteBinding wrapped, final Crypt crypt) {
        this.crypt = crypt;
        this.wrapped = wrapped;
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        return new CryptConnectionSource(wrapped.getReadConnectionSource());
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        return new CryptConnectionSource(wrapped.getWriteConnectionSource());
    }

    @Override
    public SessionContext getSessionContext() {
        return wrapped.getSessionContext();
    }

    @Override
    public int getCount() {
        return wrapped.getCount();
    }

    @Override
    public ReadWriteBinding retain() {
        wrapped.retain();
        return this;
    }

    @Override
    public void release() {
        wrapped.release();
    }

    @Override
    public Cluster getCluster() {
        return wrapped.getCluster();
    }

    private class CryptConnectionSource implements ConnectionSource {
        private final ConnectionSource wrapped;

        CryptConnectionSource(final ConnectionSource wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public ServerDescription getServerDescription() {
            return wrapped.getServerDescription();
        }

        @Override
        public SessionContext getSessionContext() {
            return wrapped.getSessionContext();
        }

        @Override
        public Connection getConnection() {
            return new CryptConnection(wrapped.getConnection(), crypt);
        }

        @Override
        public int getCount() {
            return wrapped.getCount();
        }

        @Override
        public ConnectionSource retain() {
            wrapped.retain();
            return this;
        }

        @Override
        public void release() {
            wrapped.release();
        }
    }
}
