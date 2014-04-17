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
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.ServerVersion;
import org.mongodb.protocol.DeleteProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import static java.util.Arrays.asList;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.OperationHelper.executeProtocol;
import static org.mongodb.operation.OperationHelper.executeProtocolAsync;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.getPrimaryConnectionProvider;
import static org.mongodb.operation.OperationHelper.ignoreResult;
import static org.mongodb.operation.OperationHelper.serverVersionIsAtLeast;

/**
 * An operation to remove a user.
 *
 * @since 3.0
 */
public class DropUserOperation implements AsyncOperation<Void>, Operation<Void>  {
    private final String database;
    private final String userName;

    public DropUserOperation(final String source, final String userName) {
        this.database = notNull("source", source);
        this.userName = notNull("userName", userName);
    }

    @Override
    public Void execute(final Session session) {
        ServerConnectionProvider connectionProvider = getPrimaryConnectionProvider(session);
        if (serverVersionIsAtLeast(connectionProvider, new ServerVersion(2, 6))) {
            executeWrappedCommandProtocol(database, getCommand(), connectionProvider);
        } else {
            executeProtocol(getCollectionBasedProtocol(), connectionProvider);
        }
        return null;
    }

    @Override
    public MongoFuture<Void> executeAsync(final Session session) {
        ServerConnectionProvider connectionProvider = getPrimaryConnectionProvider(session);
        if (serverVersionIsAtLeast(connectionProvider, new ServerVersion(2, 6))) {
            MongoFuture<CommandResult> result = executeWrappedCommandProtocolAsync(database, getCommand(), connectionProvider);
            return ignoreResult(result);
        } else {
            MongoFuture<WriteResult> result = executeProtocolAsync(getCollectionBasedProtocol(), session);
            return ignoreResult(result);
        }
    }

    private DeleteProtocol getCollectionBasedProtocol() {
        MongoNamespace namespace = new MongoNamespace(database, "system.users");
        return new DeleteProtocol(namespace, true, WriteConcern.ACKNOWLEDGED,
                asList(new RemoveRequest(new Document("user", userName))),
                new DocumentCodec());
    }

    private Document getCommand() {
        return new Document("dropUser", userName);
    }
}
