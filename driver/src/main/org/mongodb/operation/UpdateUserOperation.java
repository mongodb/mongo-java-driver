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
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ServerVersion;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.protocol.ReplaceProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import static java.util.Arrays.asList;
import static org.mongodb.operation.UserOperationHelper.asCollectionDocument;
import static org.mongodb.operation.UserOperationHelper.asCollectionQueryDocument;
import static org.mongodb.operation.UserOperationHelper.asCommandDocument;

/**
 * An operation to update a user.
 *
 * @since 3.0
 */
public class UpdateUserOperation extends BaseOperation<WriteResult> {
    private final User user;

    public UpdateUserOperation(final User user, final BufferProvider bufferProvider, final Session session, final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.user = user;
    }

    @Override
    @SuppressWarnings("unchecked")
    public WriteResult execute() {
        ServerConnectionProvider serverConnectionProvider =
            getPrimaryServerConnectionProvider();
        if (serverConnectionProvider.getServerDescription().getVersion().compareTo(new ServerVersion(asList(2, 5, 3))) >= 0) {
            return executeCommandBasedProtocol(serverConnectionProvider);
        } else {
            return executeCollectionBasedProtocol(serverConnectionProvider);
        }
    }

    private WriteResult executeCommandBasedProtocol(final ServerConnectionProvider serverConnectionProvider) {
        CommandProtocol commandProtocol = new CommandProtocol(user.getCredential().getSource(), asCommandDocument(user, "updateUser"),
                                                              new DocumentCodec(),
                                                              new DocumentCodec(), getBufferProvider(),
                                                              serverConnectionProvider.getServerDescription(),
                                                              serverConnectionProvider.getConnection(), true);
        CommandResult commandResult = commandProtocol.execute();
        return new WriteResult(commandResult, WriteConcern.ACKNOWLEDGED);
    }

    @SuppressWarnings("unchecked")
    private WriteResult executeCollectionBasedProtocol(final ServerConnectionProvider serverConnectionProvider) {
        return new ReplaceProtocol<Document>(new MongoNamespace(user.getCredential().getSource(), "system.users"),
                                             WriteConcern.ACKNOWLEDGED,
                                             asList(new Replace<Document>(WriteConcern.ACKNOWLEDGED,
                                                                          asCollectionQueryDocument(user),
                                                                          asCollectionDocument(user))),
                                             new DocumentCodec(),
                                             new DocumentCodec(),
                                             getBufferProvider(),
                                             serverConnectionProvider.getServerDescription(),
                                             serverConnectionProvider.getConnection(),
                                             true).execute();
    }
}