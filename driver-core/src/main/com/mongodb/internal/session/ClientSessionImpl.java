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

package com.mongodb.internal.session;

import com.mongodb.session.ClientSession;
import com.mongodb.ClientSessionOptions;
import com.mongodb.session.ServerSession;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import static com.mongodb.assertions.Assertions.isTrue;

public class ClientSessionImpl implements ClientSession {
    private static final String CLUSTER_TIME_KEY = "clusterTime";

    private final ServerSessionPool serverSessionPool;
    private final ServerSession serverSession;
    private final Object originator;
    private final ClientSessionOptions options;
    private BsonDocument clusterTime;
    private BsonTimestamp operationTime;
    private volatile boolean closed;

    public ClientSessionImpl(final ServerSessionPool serverSessionPool, final Object originator, final ClientSessionOptions options) {
        this.serverSessionPool = serverSessionPool;
        this.serverSession = serverSessionPool.get();
        this.originator = originator;
        this.options = options;
        closed = false;
    }

    @Override
    public ClientSessionOptions getOptions() {
        return options;
    }

    @Override
    public boolean isCausallyConsistent() {
        return options.isCausallyConsistent() == null ? true : options.isCausallyConsistent();
    }

    @Override
    public Object getOriginator() {
        return originator;
    }

    @Override
    public BsonDocument getClusterTime() {
        return clusterTime;
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return operationTime;
    }

    @Override
    public ServerSession getServerSession() {
        isTrue("open", !closed);
        return serverSession;
    }

    @Override
    public void advanceOperationTime(final BsonTimestamp newOperationTime) {
        isTrue("open", !closed);
        this.operationTime = greaterOf(newOperationTime);
    }

    @Override
    public void advanceClusterTime(final BsonDocument newClusterTime) {
        isTrue("open", !closed);
        this.clusterTime = greaterOf(newClusterTime);
    }

    private BsonDocument greaterOf(final BsonDocument newClusterTime) {
        if (newClusterTime == null) {
            return clusterTime;
        } else if (clusterTime == null) {
            return newClusterTime;
        } else {
            return newClusterTime.getTimestamp(CLUSTER_TIME_KEY).compareTo(clusterTime.getTimestamp(CLUSTER_TIME_KEY)) > 0
                    ? newClusterTime : clusterTime;
        }
    }

    private BsonTimestamp greaterOf(final BsonTimestamp newOperationTime) {
        if (newOperationTime == null) {
            return operationTime;
        } else if (operationTime == null) {
            return newOperationTime;
        } else {
            return newOperationTime.compareTo(operationTime) > 0 ? newOperationTime : operationTime;
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            serverSessionPool.release(serverSession);
        }
    }
}
