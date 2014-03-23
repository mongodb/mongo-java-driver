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

import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.ServerVersion;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.protocol.DeleteProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import java.util.Arrays;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.OperationHelper.executeProtocol;

/**
 * An operation to remove a user.
 *
 * @since 3.0
 */
public class DropUserOperation implements Operation<Void> {
    private final String database;
    private final String userName;

    public DropUserOperation(final String source, final String userName) {
        this.database = notNull("source", source);
        this.userName = notNull("userName", userName);
    }

    @Override
    public Void execute(final Session session) {
        ServerConnectionProvider serverConnectionProvider =
        OperationHelper.getPrimaryServerConnectionProvider(session);
        if (serverConnectionProvider.getServerDescription().getVersion().compareTo(new ServerVersion(2, 6)) >= 0) {
            executeCommandBasedProtocol(serverConnectionProvider);
        } else {
            executeCollectionBasedProtocol(serverConnectionProvider);
        }
        return null;
    }

    private void executeCommandBasedProtocol(final ServerConnectionProvider serverConnectionProvider) {
        CommandProtocol commandProtocol = new CommandProtocol(database, asCommandDocument(),
                                                              new DocumentCodec(),
                                                              new DocumentCodec());
        executeProtocol(commandProtocol, serverConnectionProvider);
    }

    private Document asCommandDocument() {
        return new Document("dropUser", userName);
    }

    private void executeCollectionBasedProtocol(final ServerConnectionProvider serverConnectionProvider) {
        MongoNamespace namespace = new MongoNamespace(database, "system.users");
        DocumentCodec codec = new DocumentCodec();
        executeProtocol(new DeleteProtocol(namespace,
                                           true, WriteConcern.ACKNOWLEDGED,
                                           Arrays.asList(new RemoveRequest(new Document("user", userName))),
                                           codec),
                        serverConnectionProvider);
    }
}
