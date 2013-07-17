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

import org.mongodb.Codec;
import org.mongodb.Document;
import org.mongodb.Operation;
import org.mongodb.command.Command;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ServerSelector;
import org.mongodb.operation.protocol.CommandProtocol;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

public class CommandOperation implements Operation<CommandResult> {
    private final Command command;
    private final Codec<Document> codec;
    private final BufferProvider bufferProvider;
    private final String database;
    private final ClusterDescription clusterDescription;
    private final Session session;
    private final boolean closeSession;

    public CommandOperation(final String database, final Command command, final Codec<Document> codec,
                            final ClusterDescription clusterDescription, final BufferProvider bufferProvider,
                            final Session session, final boolean closeSession) {
        this.database = database;
        this.clusterDescription = clusterDescription;
        this.session = session;
        this.closeSession = closeSession;
        this.bufferProvider = bufferProvider;
        this.command = command;
        this.codec = codec;
    }

    public Command getCommand() {
        return command;
    }

    public CommandResult execute() {
        try {
            ServerConnectionProvider provider = session.createServerConnectionProvider(new ServerConnectionProviderOptions(isQuery(),
                    getServerSelector()));
            return new CommandProtocol(database, command, codec, bufferProvider, provider.getServerDescription(),
                    provider.getConnection(), true).execute();
        } finally {
            if (closeSession) {
                session.close();
            }
        }
    }

    private ServerSelector getServerSelector() {
        return new ReadPreferenceServerSelector(CommandReadPreferenceHelper.getCommandReadPreference(command, clusterDescription));
    }

    private boolean isQuery() {
        return CommandReadPreferenceHelper.isQuery(command);
    }
}
