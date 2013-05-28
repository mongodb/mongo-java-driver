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

import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.connection.ServerConnection;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

/**
 * @since 3.0
 */
@NotThreadSafe
class SingleConnectionSession implements Session {
    private final ServerConnection connection;
    private boolean isClosed;

    public SingleConnectionSession(final ServerConnection connection) {
        this.connection = notNull("connection", connection);
    }

    @Override
    public ServerConnection getConnection() {
        isTrue("open", !isClosed());
        return new DelayedCloseConnection(connection);
    }

    @Override
    public void close() {
        connection.close();
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
