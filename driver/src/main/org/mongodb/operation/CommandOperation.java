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
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.ReadPreference;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ServerSelector;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import static org.mongodb.operation.CommandReadPreferenceHelper.getCommandReadPreference;
import static org.mongodb.operation.CommandReadPreferenceHelper.isQuery;
import static org.mongodb.operation.OperationHelper.executeProtocol;
import static org.mongodb.operation.OperationHelper.getConnectionAsync;

public class CommandOperation implements AsyncOperation<CommandResult>, Operation<CommandResult> {
    private final Encoder<Document> commandEncoder;
    private final Decoder<Document> commandDecoder;
    private final String database;
    private final ClusterDescription clusterDescription;
    private final Document commandDocument;
    private final ReadPreference readPreference;

    public CommandOperation(final String database, final Document command, final ReadPreference readPreference,
                            final Decoder<Document> commandDecoder, final Encoder<Document> commandEncoder,
                            final ClusterDescription clusterDescription) {
        super();
        this.database = database;
        this.clusterDescription = clusterDescription;
        this.commandEncoder = commandEncoder;
        this.commandDecoder = commandDecoder;
        this.commandDocument = command;
        this.readPreference = readPreference;
    }

    CommandOperation(final String database, final Document command, final ReadPreference readPreference,
                     final Decoder<Document> commandDecoder, final Encoder<Document> commandEncoder) {
        this(database, command, readPreference, commandDecoder, commandEncoder, null);
    }

    @Override
    public CommandResult execute(final Session session) {
        ServerConnectionProviderOptions options = getServerConnectionProviderOptions();
        ServerConnectionProvider provider = session.createServerConnectionProvider(options);
        return executeProtocol(new CommandProtocol(database, commandDocument, commandEncoder, commandDecoder), provider);
    }

    @Override
    public MongoFuture<CommandResult> executeAsync(final Session session) {
        final SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();
        getConnectionAsync(session, new ServerConnectionProviderOptions(false, new PrimaryServerSelector()))
        .register(new SingleResultCallback<ServerDescriptionConnectionPair>() {
            @Override
            public void onResult(final ServerDescriptionConnectionPair pair, final MongoException e) {
                new CommandProtocol(database, commandDocument, commandEncoder, commandDecoder)
                .executeAsync(pair.getConnection(), pair.getServerDescription())
                .register(new SessionClosingSingleResultCallback<CommandResult>(retVal));
            }
        });
        return retVal;
    }

    private ServerConnectionProviderOptions getServerConnectionProviderOptions() {
        return new ServerConnectionProviderOptions(isQuery(commandDocument), getServerSelector());
    }

    private ServerSelector getServerSelector() {
        return new ReadPreferenceServerSelector(clusterDescription == null
                                                ? readPreference
                                                : getCommandReadPreference(commandDocument, readPreference, clusterDescription));
    }
}
