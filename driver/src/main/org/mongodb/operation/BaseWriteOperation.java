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
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerVersion;
import org.mongodb.operation.protocol.WriteCommandProtocol;
import org.mongodb.operation.protocol.WriteProtocol;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import java.util.Arrays;

import static org.mongodb.assertions.Assertions.notNull;

public abstract class BaseWriteOperation extends BaseOperation<CommandResult> {

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
    public CommandResult execute() {
        ServerConnectionProvider provider = getSession().createServerConnectionProvider(
                new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));
        Connection connection = provider.getConnection();
        try {
            if (writeConcern.isAcknowledged()
                    && provider.getServerDescription().getVersion().compareTo(new ServerVersion(Arrays.asList(2, 5, 4))) >= 0) {
                return getCommandProtocol(provider.getServerDescription(), connection).execute();
            }
            else {
                return getWriteProtocol(provider.getServerDescription(), connection).execute();
            }
        } finally {
            connection.close();
            if (isCloseSession()) {
                getSession().close();
            }
        }
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    protected abstract WriteProtocol getWriteProtocol(final ServerDescription serverDescription, Connection connection);

    protected abstract WriteCommandProtocol getCommandProtocol(final ServerDescription serverDescription, final Connection connection);
}
