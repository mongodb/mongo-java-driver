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

package org.mongodb.connection;


import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.async.SingleResultFuture;

import java.util.concurrent.Executor;

import static org.mongodb.assertions.Assertions.isTrue;

public class SingleServerAsyncSession extends AbstractAsyncBaseSession implements AsyncSession {
    private final Server server;

    public SingleServerAsyncSession(final Server server, final Cluster cluster, final Executor executor) {
        super(cluster, executor);
        this.server = server;
    }

    @Override
    public MongoFuture<AsyncConnection> getConnection(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());
        return getConnection();
    }

    @Override
    public MongoFuture<AsyncConnection> getConnection() {
        isTrue("open", !isClosed());

        final SingleResultFuture<AsyncConnection> retVal = new SingleResultFuture<AsyncConnection>();

        getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    AsyncConnection connection = server.getAsyncConnection();
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
}
