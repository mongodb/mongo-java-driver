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

package com.mongodb.async.client.internal;

import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadWriteBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AbstractReferenceCounted;
import com.mongodb.internal.binding.AsyncClusterAwareReadWriteBinding;
import com.mongodb.session.SessionContext;

@SuppressWarnings("deprecation")
public class AsyncCryptBinding implements AsyncClusterAwareReadWriteBinding {
    private final AsyncClusterAwareReadWriteBinding wrapped;
    private final Crypt crypt;

    public AsyncCryptBinding(final AsyncClusterAwareReadWriteBinding wrapped, final Crypt crypt) {
        this.wrapped = wrapped;
        this.crypt = crypt;
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
                    callback.onResult(new AsyncCryptConnectionSource(result), null);
                }
            }
        });
    }

    @Override
    public SessionContext getSessionContext() {
        return wrapped.getSessionContext();
    }

    @Override
    public void getReadConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        wrapped.getReadConnectionSource(new SingleResultCallback<AsyncConnectionSource>() {
            @Override
            public void onResult(final AsyncConnectionSource result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(new AsyncCryptConnectionSource(result), null);
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

    @Override
    public Cluster getCluster() {
        return wrapped.getCluster();
    }

    private class AsyncCryptConnectionSource extends AbstractReferenceCounted implements AsyncConnectionSource {
        private final AsyncConnectionSource wrapped;

        AsyncCryptConnectionSource(final AsyncConnectionSource wrapped) {
            this.wrapped = wrapped;
            AsyncCryptBinding.this.retain();
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
        public void getConnection(final SingleResultCallback<AsyncConnection> callback) {
            wrapped.getConnection(new SingleResultCallback<AsyncConnection>() {
                @Override
                public void onResult(final AsyncConnection result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(new AsyncCryptConnection(result, crypt), null);
                    }
                }
            });
        }

        @Override
        public AsyncConnectionSource retain() {
            wrapped.retain();
            return this;
        }

        @Override
        public void release() {
            wrapped.release();
            if (wrapped.getCount() == 0) {
                AsyncCryptBinding.this.release();
            }
        }
    }
}
