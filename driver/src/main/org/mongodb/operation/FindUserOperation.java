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

package org.mongodb.operation;

import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.protocol.QueryProtocol;
import org.mongodb.operation.protocol.QueryResult;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import static org.mongodb.assertions.Assertions.notNull;

public class FindUserOperation extends BaseOperation<Document> {

    private final String database;
    private final String userName;

    public FindUserOperation(final String database, final BufferProvider bufferProvider, final String userName, final Session session,
                             final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.database = notNull("database", database);
        this.userName = notNull("userName", userName);
    }

    @Override
    public Document execute() {
        ServerConnectionProvider serverConnectionProvider = getSession().createServerConnectionProvider(
                new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));
        MongoNamespace namespace = new MongoNamespace(database, "system.users");
        DocumentCodec codec = new DocumentCodec();
        QueryResult<Document> result = new QueryProtocol<Document>(namespace,
                new Find(new Document("user", userName)), codec, codec, getBufferProvider(),
                serverConnectionProvider.getServerDescription(), serverConnectionProvider.getConnection(), true).execute();
        if (result.getResults().isEmpty()) {
            return null;
        }
        else {
            return result.getResults().get(0);
        }
    }
}
