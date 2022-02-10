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

package com.mongodb.reactivestreams.client.internal.crypt;

import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;

public class CryptBinding implements AsyncClusterAwareReadWriteBinding {

    private final AsyncClusterAwareReadWriteBinding wrapped;
    private final Crypt crypt;

    public CryptBinding(final AsyncClusterAwareReadWriteBinding wrapped, final Crypt crypt) {
        this.wrapped = wrapped;
        this.crypt = crypt;
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public void getWriteConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        wrapped.getWriteConnectionSource((result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                callback.onResult(new CryptConnectionSource(result), null);
            }
        });
    }

    @Override
    public SessionContext getSessionContext() {
        return wrapped.getSessionContext();
    }

    @Override
    @Nullable
    public ServerApi getServerApi() {
        return wrapped.getServerApi();
    }

    @Override
    @Nullable
    public RequestContext getRequestContext() {
        return wrapped.getRequestContext();
    }

    @Override
    public void getReadConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        wrapped.getReadConnectionSource((result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                callback.onResult(new CryptConnectionSource(result), null);
            }
        });
    }


    @Override
    public void getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference,
            final SingleResultCallback<AsyncConnectionSource> callback) {
        wrapped.getReadConnectionSource(minWireVersion, fallbackReadPreference, (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                callback.onResult(new CryptConnectionSource(result), null);
            }
        });
    }


    @Override
    public void getConnectionSource(final ServerAddress serverAddress, final SingleResultCallback<AsyncConnectionSource> callback) {
        wrapped.getConnectionSource(serverAddress, (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                callback.onResult(new CryptConnectionSource(result), null);
            }
        });
    }

    @Override
    public int getCount() {
        return wrapped.getCount();
    }

    @Override
    public AsyncClusterAwareReadWriteBinding retain() {
        wrapped.retain();
        return this;
    }

    @Override
    public int release() {
        return wrapped.release();
    }

    @Override
    public Cluster getCluster() {
        return wrapped.getCluster();
    }

    private class CryptConnectionSource implements AsyncConnectionSource {
        private final AsyncConnectionSource wrapped;

        CryptConnectionSource(final AsyncConnectionSource wrapped) {
            this.wrapped = wrapped;
            CryptBinding.this.retain();
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
        @Nullable
        public ServerApi getServerApi() {
            return wrapped.getServerApi();
        }

        @Override
        public RequestContext getRequestContext() {
            return wrapped.getRequestContext();
        }

        @Override
        public ReadPreference getReadPreference() {
            return wrapped.getReadPreference();
        }

        @Override
        public void getConnection(final SingleResultCallback<AsyncConnection> callback) {
            wrapped.getConnection((result, t) -> {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(new CryptConnection(result, crypt), null);
                }
            });
        }

        @Override
        public int getCount() {
            return wrapped.getCount();
        }

        @Override
        public AsyncConnectionSource retain() {
            wrapped.retain();
            return this;
        }

        @Override
        public int release() {
            return wrapped.release();
        }
    }
}
