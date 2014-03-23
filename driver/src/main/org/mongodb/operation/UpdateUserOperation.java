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
import org.mongodb.protocol.ReplaceProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import static java.util.Arrays.asList;
import static org.mongodb.operation.OperationHelper.executeProtocol;
import static org.mongodb.operation.UserOperationHelper.asCollectionDocument;
import static org.mongodb.operation.UserOperationHelper.asCollectionQueryDocument;
import static org.mongodb.operation.UserOperationHelper.asCommandDocument;

/**
 * An operation to update a user.
 *
 * @since 3.0
 */
public class UpdateUserOperation implements Operation<Void> {
    private final User user;

    public UpdateUserOperation(final User user) {
        this.user = user;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void execute(final Session session) {
        ServerConnectionProvider serverConnectionProvider = OperationHelper.getPrimaryServerConnectionProvider(session);
        if (serverConnectionProvider.getServerDescription().getVersion().compareTo(new ServerVersion(2, 6)) >= 0) {
            executeCommandBasedProtocol(serverConnectionProvider);
        } else {
            executeCollectionBasedProtocol(serverConnectionProvider);
        }
        return null;
    }

    private void executeCommandBasedProtocol(final ServerConnectionProvider serverConnectionProvider) {
        CommandProtocol commandProtocol = new CommandProtocol(user.getCredential().getSource(), asCommandDocument(user, "updateUser"),
                                                              new DocumentCodec(),
                                                              new DocumentCodec());
        executeProtocol(commandProtocol, serverConnectionProvider);
    }

    @SuppressWarnings("unchecked")
    private void executeCollectionBasedProtocol(final ServerConnectionProvider serverConnectionProvider) {
        executeProtocol(new ReplaceProtocol<Document>(new MongoNamespace(user.getCredential().getSource(), "system.users"),
                                                      true, WriteConcern.ACKNOWLEDGED,
                                                      asList(new ReplaceRequest<Document>(asCollectionQueryDocument(user),
                                                                                          asCollectionDocument(user))),
                                                      new DocumentCodec(),
                                                      new DocumentCodec()),
                        serverConnectionProvider);
    }
}