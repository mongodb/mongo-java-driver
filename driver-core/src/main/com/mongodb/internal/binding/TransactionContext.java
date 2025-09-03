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

import com.mongodb.connection.ClusterType;
import com.mongodb.internal.connection.Connection;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;

import java.util.function.BiConsumer;

import static com.mongodb.connection.ClusterType.LOAD_BALANCED;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class TransactionContext<C extends ReferenceCounted> extends AbstractReferenceCounted {
    private final ClusterType clusterType;
    private C pinnedConnection;

    public TransactionContext(final ClusterType clusterType) {
        this.clusterType = clusterType;
    }

    @Nullable
    public C getPinnedConnection() {
        return pinnedConnection;
    }

    @SuppressWarnings("unchecked")
    public void pinConnection(final C connection, final BiConsumer<C, Connection.PinningMode> markAsPinnedOperation) {
        this.pinnedConnection = (C) connection.retain(); // safe because of the `retain` method contract
        markAsPinnedOperation.accept(connection, Connection.PinningMode.TRANSACTION);
    }

    public boolean isConnectionPinningRequired() {
        return clusterType == LOAD_BALANCED;
    }

    @Override
    public int release() {
        int count = super.release();
        if (count == 0) {
            if (pinnedConnection != null) {
                pinnedConnection.release();
            }
        }
        return count;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static <C extends TransactionContext<? extends ReferenceCounted>> C get(final ClientSession session) {
        return (C) session.getTransactionContext();
    }
}
