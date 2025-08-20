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
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;

import static org.bson.assertions.Assertions.notNull;

public class OperationContextBinding implements ReadWriteBinding {
    private final ReadWriteBinding wrapped;
    private final OperationContext operationContext;

    public OperationContextBinding(final ReadWriteBinding wrapped, final OperationContext operationContext) {
        this.wrapped = notNull("wrapped", wrapped);
        this.operationContext = notNull("operationContext", operationContext);
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
    public int release() {
        return wrapped.release();
    }

    @Override
    public ConnectionSource getReadConnectionSource(final OperationContext operationContext) {
        return new SessionBindingConnectionSource(wrapped.getReadConnectionSource(operationContext));
    }

    @Override
    public ConnectionSource getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference, final OperationContext operationContext) {
        return new SessionBindingConnectionSource(wrapped.getReadConnectionSource(minWireVersion, fallbackReadPreference, operationContext));
    }

    public OperationContext getOperationContext() {
        return operationContext;
    }

    @Override
    public ConnectionSource getWriteConnectionSource(final OperationContext operationContext) {
        return new SessionBindingConnectionSource(wrapped.getWriteConnectionSource(operationContext));
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
        public ReadPreference getReadPreference() {
            return wrapped.getReadPreference();
        }

        @Override
        public Connection getConnection(final OperationContext operationContext) {
            return wrapped.getConnection(operationContext);
        }

        @Override
        public ConnectionSource retain() {
            wrapped = wrapped.retain();
            return this;
        }

        @Override
        public int getCount() {
            return wrapped.getCount();
        }

        @Override
        public int release() {
            return wrapped.release();
        }
    }

    public ReadWriteBinding getWrapped() {
        return wrapped;
    }
}
