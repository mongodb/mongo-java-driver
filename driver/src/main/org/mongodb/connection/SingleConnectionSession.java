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

import org.mongodb.ReadPreference;

import static org.mongodb.assertions.Assertions.isTrue;

public class SingleConnectionSession extends AbstractBaseSession implements Session {
    private Connection connection;

    public SingleConnectionSession(final Connection connection, final Cluster cluster) {
        super(cluster);
        this.connection = connection;
    }

    @Override
    public Connection getConnection(final ReadPreference readPreference) {
        isTrue("open", !isClosed());
        return new DelayedCloseConnection(connection);
    }

    @Override
    public Connection getConnection() {
        isTrue("open", !isClosed());
        return new DelayedCloseConnection(connection);
    }

    /**
     * Closes the session and the connection bound to this session.
     */
    @Override
    public void close() {
        connection.close();
        super.close();
    }
}
