/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.session;


import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.Server;
import org.mongodb.connection.ServerSelector;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.async.SingleResultFuture;

import java.util.concurrent.Executor;

import static org.mongodb.assertions.Assertions.isTrue;

/**
 *
 * @since 3.0
 */
@NotThreadSafe
class SingleServerAsyncSession implements AsyncSession {
    private final ServerSelector serverSelector;
    private final Cluster cluster;
    private final Executor executor;
    private Server server;
    private boolean isClosed;

    public SingleServerAsyncSession(final ServerSelector serverSelector, final Cluster cluster, final Executor executor) {
        this.serverSelector = serverSelector;
        this.cluster = cluster;
        this.executor = executor;
    }

    @Override
    public MongoFuture<AsyncConnection> getConnection() {
        isTrue("open", !isClosed());

        final SingleResultFuture<AsyncConnection> retVal = new SingleResultFuture<AsyncConnection>();

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    AsyncConnection connection = getServer().getAsyncConnection();
                    retVal.init(connection, null);
                } catch (MongoException e) {
                    retVal.init(null, e);
                } catch (Throwable t) {
                    retVal.init(null, new MongoInternalException("Exception getting a connection", t));
                }
            }
        });

        return retVal;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private synchronized Server getServer() {
        if (server == null) {
            server = cluster.getServer(serverSelector);
        }
        return server;
    }
}
