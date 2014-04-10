/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import org.mongodb.connection.ServerVersion;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import java.util.EnumSet;
import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.OperationHelper.executeProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;

/**
 * An operation to determine if a user exists.
 *
 * @since 3.0
 */
public class UserExistsOperation implements Operation<Boolean> {

    private final String database;
    private final String userName;

    public UserExistsOperation(final String source, final String userName) {
        this.database = notNull("source", source);
        this.userName = notNull("userName", userName);
    }

    @Override
    public Boolean execute(final Session session) {
        ServerConnectionProvider provider = OperationHelper.getPrimaryConnectionProvider(session);
        if (provider.getServerDescription().getVersion().compareTo(new ServerVersion(2, 6)) >= 0) {
            return executeCommandBasedProtocol(provider);
        } else {
            return executeCollectionBasedProtocol(provider);
        }
    }

    private Boolean executeCommandBasedProtocol(final ServerConnectionProvider serverConnectionProvider) {
        CommandResult commandResult = executeWrappedCommandProtocol(database, new Document("usersInfo", userName), new DocumentCodec(),
                                                                    new DocumentCodec(), serverConnectionProvider);

        return !commandResult.getResponse().get("users", List.class).isEmpty();
    }

    private Boolean executeCollectionBasedProtocol(final ServerConnectionProvider serverConnectionProvider) {
        MongoNamespace namespace = new MongoNamespace(database, "system.users");
        DocumentCodec codec = new DocumentCodec();
        QueryResult<Document> result = executeProtocol(new QueryProtocol<Document>(namespace, EnumSet.noneOf(QueryFlag.class), 0, 1,
                                                                                   new Document("user", userName), null,
                                                                                   codec, codec),
                                                       serverConnectionProvider);
        return !result.getResults().isEmpty();
    }
}
