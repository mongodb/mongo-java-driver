/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.binding;

import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.SessionContext;

import static org.bson.assertions.Assertions.notNull;

public final class AsyncSessionBinding implements AsyncReadWriteBinding {

    private final AsyncReadWriteBinding wrapped;
    private final SessionContext sessionContext;

    AsyncSessionBinding(final AsyncReadWriteBinding wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
        this.sessionContext = new SimpleSessionContext();
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public void getWriteConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        wrapped.getWriteConnectionSource(new SingleResultCallback<AsyncConnectionSource>() {
            @Override
            public void onResult(final AsyncConnectionSource result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(new SessionBindingAsyncConnectionSource(result), null);
                }
            }
        });
    }

    @Override
    public SessionContext getSessionContext() {
        return sessionContext;
    }

    @Override
    public void getReadConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        wrapped.getReadConnectionSource(new SingleResultCallback<AsyncConnectionSource>() {
            @Override
            public void onResult(final AsyncConnectionSource result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(new SessionBindingAsyncConnectionSource(result), null);
                }
            }
        });
    }

    @Override
    public int getCount() {
        return wrapped.getCount();
    }

    @Override
    public AsyncReadWriteBinding retain() {
        return wrapped.retain();
    }

    @Override
    public void release() {
        wrapped.release();
    }

    private class SessionBindingAsyncConnectionSource implements AsyncConnectionSource {
        private AsyncConnectionSource wrapped;

        SessionBindingAsyncConnectionSource(final AsyncConnectionSource wrapped) {
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
        public void getConnection(final SingleResultCallback<AsyncConnection> callback) {
            wrapped.getConnection(callback);
        }

        @Override
        public int getCount() {
            return wrapped.getCount();
        }

        @Override
        public AsyncConnectionSource retain() {
            return wrapped.retain();
        }

        @Override
        public void release() {
            wrapped.release();
        }
    }

    public AsyncReadWriteBinding getWrapped() {
        return wrapped;
    }
}
