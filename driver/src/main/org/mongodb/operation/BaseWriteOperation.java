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
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Channel;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerVersion;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.WriteCommandProtocol;
import org.mongodb.protocol.WriteProtocol;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerChannelProvider;
import org.mongodb.session.ServerChannelProviderOptions;
import org.mongodb.session.Session;

import java.util.Arrays;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.OperationHelper.getChannelAsync;

public abstract class BaseWriteOperation extends BaseOperation<CommandResult> implements AsyncOperation<CommandResult> {

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
        ServerChannelProvider provider = getSession().createServerChannelProvider(
                new ServerChannelProviderOptions(false, new PrimaryServerSelector()));
        Channel channel = provider.getChannel();
        try {
            if (writeConcern.isAcknowledged()
                    && provider.getServerDescription().getVersion().compareTo(new ServerVersion(Arrays.asList(2, 5, 4))) >= 0) {
                return getCommandProtocol(provider.getServerDescription(), channel).execute();
            }
            else {
                return getWriteProtocol(provider.getServerDescription(), channel).execute();
            }
        } finally {
            channel.close();
            if (isCloseSession()) {
                getSession().close();
            }
        }
    }

    @Override
    public MongoFuture<CommandResult> executeAsync() {
        final SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();
        getChannelAsync(getSession(), new ServerChannelProviderOptions(false, new PrimaryServerSelector()))
                .register(new SingleResultCallback<ServerDescriptionChannelPair>() {
                    @Override
                    public void onResult(final ServerDescriptionChannelPair pair, final MongoException e) {
                        MongoFuture<CommandResult> protocolFuture;
                        if (writeConcern.isAcknowledged()
                                && pair.getServerDescription().getVersion().compareTo(new ServerVersion(Arrays.asList(2, 5, 4))) >= 0) {
                            protocolFuture = getCommandProtocol(pair.getServerDescription(), pair.getChannel()).executeAsync();
                        }
                        else {
                            protocolFuture = getWriteProtocol(pair.getServerDescription(), pair.getChannel()).executeAsync();
                        }
                        protocolFuture.register(
                                new SessionClosingSingleResultCallback<CommandResult>(retVal, getSession(), isCloseSession()));
                    }
                });
        return retVal;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    protected abstract WriteProtocol getWriteProtocol(final ServerDescription serverDescription, Channel channel);

    protected abstract WriteCommandProtocol getCommandProtocol(final ServerDescription serverDescription, final Channel channel);
}
