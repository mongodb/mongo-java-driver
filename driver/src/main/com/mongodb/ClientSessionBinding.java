/*
 * Copyright 2017 MongoDB, Inc.
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
 *
 */

package com.mongodb;

import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.SessionContext;

import static org.bson.assertions.Assertions.notNull;

class ClientSessionBinding implements ReadWriteBinding {
    private final ReadWriteBinding wrapped;
    private final ClientSession session;
    private final boolean ownsSession;
    private final ClientSessionContext sessionContext;

    ClientSessionBinding(final ClientSession session, final boolean ownsSession, final ReadWriteBinding wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
        this.ownsSession = ownsSession;
        this.session = notNull("session", session);
        this.sessionContext = new ClientSessionContext(session);
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
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
        closeSessionIfCountIsZero();
    }

    private void closeSessionIfCountIsZero() {
        if (getCount() == 0 && ownsSession) {
            session.close();
        }
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        return new SessionBindingConnectionSource(wrapped.getReadConnectionSource());
    }

    @Override
    public SessionContext getSessionContext() {
        return sessionContext;
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        return new SessionBindingConnectionSource(wrapped.getWriteConnectionSource());
    }

    private class SessionBindingConnectionSource implements ConnectionSource {
        private ConnectionSource wrapped;

        SessionBindingConnectionSource(final ConnectionSource wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public ServerDescription getServerDescription() {
            return wrapped.getServerDescription();
        }

        @Override
        public SessionContext getSessionContext() {
            return sessionContext;
        }

        @Override
        public Connection getConnection() {
            return wrapped.getConnection();
        }

        @Override
        @SuppressWarnings("checkstyle:methodlength")
        public ConnectionSource retain() {
            wrapped = wrapped.retain();
            return this;
        }

        @Override
        public int getCount() {
            return wrapped.getCount();
        }

        @Override
        public void release() {
            wrapped.release();
            closeSessionIfCountIsZero();
        }
    }

}
