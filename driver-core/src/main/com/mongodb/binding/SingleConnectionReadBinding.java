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

package com.mongodb.binding;

import com.mongodb.ReadPreference;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AbstractReferenceCounted;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.session.SessionContext;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A read binding that is bound to a single connection.
 *
 * @since 3.2
 */
@Deprecated
public class SingleConnectionReadBinding extends AbstractReferenceCounted implements ReadBinding {

    private final ReadPreference readPreference;
    private final ServerDescription serverDescription;
    private final Connection connection;

    /**
     * Construct an instance.
     *
     * @param readPreference the read preference of this binding
     * @param serverDescription the description of the server
     * @param connection the connection to bind to.
     */
    public SingleConnectionReadBinding(final ReadPreference readPreference, final ServerDescription serverDescription,
                                       final Connection connection) {
        this.readPreference = notNull("readPreference", readPreference);
        this.serverDescription = notNull("serverDescription", serverDescription);
        this.connection = notNull("connection", connection).retain();
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        return new SingleConnectionSource();
    }

    @Override
    public SessionContext getSessionContext() {
        return NoOpSessionContext.INSTANCE;
    }

    @Override
    public ReadBinding retain() {
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

    private class SingleConnectionSource extends AbstractReferenceCounted implements ConnectionSource {

        SingleConnectionSource() {
            SingleConnectionReadBinding.this.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            return serverDescription;
        }

        @Override
        public SessionContext getSessionContext() {
            return NoOpSessionContext.INSTANCE;
        }

        @Override
        public Connection getConnection() {
            return connection.retain();
        }

        @Override
        public ConnectionSource retain() {
            super.retain();
            return this;
        }

        @Override
        public void release() {
            super.release();
            if (super.getCount() == 0) {
                SingleConnectionReadBinding.this.release();
            }
        }
    }
}
