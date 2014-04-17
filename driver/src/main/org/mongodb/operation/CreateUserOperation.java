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
import org.mongodb.binding.WriteBinding;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerVersion;
import org.mongodb.protocol.InsertProtocol;
import org.mongodb.protocol.Protocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import static java.util.Arrays.asList;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.OperationHelper.executeProtocolAsync;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.getPrimaryConnectionProvider;
import static org.mongodb.operation.OperationHelper.ignoreResult;
import static org.mongodb.operation.OperationHelper.serverVersionIsAtLeast;
import static org.mongodb.operation.OperationHelper.withConnection;
import static org.mongodb.operation.UserOperationHelper.asCollectionDocument;
import static org.mongodb.operation.UserOperationHelper.asCommandDocument;

/**
 * An operation to create a user.
 *
 * @since 3.0
 */
public class CreateUserOperation implements AsyncOperation<Void>, WriteOperation<Void> {
    private final User user;

    public CreateUserOperation(final User user) {
        this.user = notNull("user", user);
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                if (serverVersionIsAtLeast(connection, new ServerVersion(2, 6))) {
                    executeWrappedCommandProtocol(user.getCredential().getSource(), getCommand(), connection);
                } else {
                    getCollectionBasedProtocol().execute(connection);
                }
                return null;
            }
        });
    }

    @Override
    public MongoFuture<Void> executeAsync(final Session session) {
        ServerConnectionProvider connectionProvider = getPrimaryConnectionProvider(session);
        if (serverVersionIsAtLeast(connectionProvider, new ServerVersion(2, 6))) {
            MongoFuture<CommandResult> result = executeWrappedCommandProtocolAsync(user.getCredential().getSource(), getCommand(),
                                                                                   connectionProvider);
            return ignoreResult(result);
        } else {
            MongoFuture<WriteResult> result = executeProtocolAsync(getCollectionBasedProtocol(), session);
            return ignoreResult(result);
        }
    }

    @SuppressWarnings("unchecked")
    private Protocol<WriteResult> getCollectionBasedProtocol() {
        MongoNamespace namespace = new MongoNamespace(user.getCredential().getSource(), "system.users");
        return new InsertProtocol<Document>(namespace, true, WriteConcern.ACKNOWLEDGED,
                asList(new InsertRequest<Document>(asCollectionDocument(user))),
                new DocumentCodec());
    }

    private Document getCommand() {
        return asCommandDocument(user, "createUser");
    }
}
