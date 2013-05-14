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

package org.mongodb.async;

import org.mongodb.AbstractBaseSession;
import org.mongodb.Cluster;
import org.mongodb.ReadPreference;
import org.mongodb.impl.AsyncConnection;
import org.mongodb.impl.DelayedCloseAsyncConnection;

import static org.mongodb.assertions.Assertions.isTrue;

public class SingleConnectionAsyncSession extends AbstractBaseSession implements AsyncSession {
    private final AsyncConnection connection;

    public SingleConnectionAsyncSession(final AsyncConnection connection, final Cluster cluster) {
        super(cluster);
        this.connection = connection;
    }

    @Override
    public AsyncConnection getConnection(final ReadPreference readPreference) {
        isTrue("open", !isClosed());
        return new DelayedCloseAsyncConnection(connection);
    }

    @Override
    public AsyncConnection getConnection() {
        isTrue("open", !isClosed());
        return new DelayedCloseAsyncConnection(connection);
    }

    public void close() {
        if (!isClosed()) {
            connection.close();
            super.close();
        }
    }
}
