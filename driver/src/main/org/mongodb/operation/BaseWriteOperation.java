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

import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerVersion;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.WriteCommandProtocol;
import org.mongodb.protocol.WriteProtocol;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import java.util.Arrays;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.OperationHelper.getConnectionAsync;

public abstract class BaseWriteOperation extends BaseOperation<WriteResult> implements AsyncOperation<WriteResult> {

    private final WriteConcern writeConcern;
    private final MongoNamespace namespace;

    public BaseWriteOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final BufferProvider bufferProvider,
                              final Session session, final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.namespace = notNull("namespace", namespace);
        this.writeConcern = notNull("writeConcern", writeConcern);
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public WriteResult execute() {
        ServerConnectionProvider provider = getPrimaryServerConnectionProvider();
        Connection connection = provider.getConnection();
        try {
            if (writeConcern.isAcknowledged()
                && provider.getServerDescription().getVersion().compareTo(new ServerVersion(Arrays.asList(2, 8, 0))) >= 0) {
                return getCommandProtocol(provider.getServerDescription(), connection).execute();
            } else {
                return getWriteProtocol(provider.getServerDescription(), connection).execute();
            }
        } finally {
            connection.close();
            if (isCloseSession()) {
                getSession().close();
            }
        }
    }

    @Override
    public MongoFuture<WriteResult> executeAsync() {
        final SingleResultFuture<WriteResult> retVal = new SingleResultFuture<WriteResult>();
        getConnectionAsync(getSession(), new ServerConnectionProviderOptions(false, new PrimaryServerSelector()))
            .register(new SingleResultCallback<ServerDescriptionConnectionPair>() {
                @Override
                public void onResult(final ServerDescriptionConnectionPair pair, final MongoException e) {
                    MongoFuture<WriteResult> protocolFuture;
                    if (writeConcern.isAcknowledged()
                        && pair.getServerDescription().getVersion().compareTo(new ServerVersion(Arrays.asList(2, 8, 0))) >= 0) {
                        protocolFuture = getCommandProtocol(pair.getServerDescription(), pair.getConnection()).executeAsync();
                    } else {
                        protocolFuture = getWriteProtocol(pair.getServerDescription(), pair.getConnection()).executeAsync();
                    }
                    protocolFuture.register(
                                               new SessionClosingSingleResultCallback<WriteResult>(retVal, getSession(), isCloseSession()));
                }
            });
        return retVal;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    protected abstract WriteProtocol getWriteProtocol(ServerDescription serverDescription, Connection connection);

    protected abstract WriteCommandProtocol getCommandProtocol(ServerDescription serverDescription, Connection connection);
}
