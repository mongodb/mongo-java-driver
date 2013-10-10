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

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ServerVersion;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import java.util.List;

import static java.util.Arrays.asList;
import static org.mongodb.assertions.Assertions.notNull;

/**
 * An operation to determine if a user exists.
 *
 * @since 3.0
 */
public class UserExistsOperation extends BaseOperation<Boolean> {

    private final String database;
    private final String userName;

    public UserExistsOperation(final String source, final String userName, final BufferProvider bufferProvider,
                               final Session session, final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.database = notNull("source", source);
        this.userName = notNull("userName", userName);
    }

    @Override
    public Boolean execute() {
        ServerConnectionProvider provider = getPrimaryServerConnectionProvider();
        if (provider.getServerDescription().getVersion().compareTo(new ServerVersion(asList(2, 5, 3))) >= 0) {
            return executeCommandBasedProtocol(provider);
        } else {
            return executeCollectionBasedProtocol(provider);
        }
    }

    private Boolean executeCommandBasedProtocol(final ServerConnectionProvider serverConnectionProvider) {
        CommandResult commandResult = new CommandProtocol(database, new Document("usersInfo", userName), new DocumentCodec(),
                                                              new DocumentCodec(), getBufferProvider(),
                                                              serverConnectionProvider.getServerDescription(),
                                                              serverConnectionProvider.getConnection(), true)
                                          .execute();
        return !commandResult.getResponse().get("users", List.class).isEmpty();
    }

    private Boolean executeCollectionBasedProtocol(final ServerConnectionProvider serverConnectionProvider) {
        MongoNamespace namespace = new MongoNamespace(database, "system.users");
        DocumentCodec codec = new DocumentCodec();
        QueryResult<Document> result = new QueryProtocol<Document>(namespace,
                                                                   new Find(new Document("user", userName)),
                                                                   codec,
                                                                   codec,
                                                                   getBufferProvider(),
                                                                   serverConnectionProvider.getServerDescription(),
                                                                   serverConnectionProvider.getConnection(),
                                                                   true)
                                           .execute();
        return !result.getResults().isEmpty();
    }
}
