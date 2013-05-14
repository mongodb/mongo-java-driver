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

package org.mongodb;

import org.mongodb.impl.MongoSyncConnection;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

public class SingleServerSession extends AbstractSession {
    private MongoServer server;

    public SingleServerSession(final MongoServer server, final Cluster cluster) {
        super(cluster);
        this.server = notNull("server", server);
    }

    /**
     * Get a connection from the server bound to this session
     * @param readPreference in this implementation, read preference is ignored.  It's assumed that the server bound to this session was
     *                       already checked to ensure it satisfies the read preference.
     * @return a connection from the bound server
     */
    @Override
    public MongoSyncConnection getConnection(final ReadPreference readPreference) {
        isTrue("open", !isClosed());
        return server.getConnection();
    }

    @Override
    public MongoSyncConnection getConnection() {
        isTrue("open", !isClosed());
        return server.getConnection();
    }
}
