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
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerChannelProvider;
import org.mongodb.session.ServerChannelProviderOptions;
import org.mongodb.session.Session;

public class DropIndexOperation extends BaseOperation<CommandResult> {
    private final Codec<Document> commandCodec = new DocumentCodec();
    private final MongoNamespace namespace;
    private final Document dropIndexesCommand;

    public DropIndexOperation(final MongoNamespace namespace, final String indexName, final BufferProvider bufferProvider,
                              final Session session, final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.namespace = namespace;
        this.dropIndexesCommand = new Document("dropIndexes", namespace.getCollectionName()).append("index", indexName);
    }

    @Override
    public CommandResult execute() {
        final ServerChannelProvider provider = getServerChannelProvider();
        try {
            return new CommandProtocol(namespace.getDatabaseName(), dropIndexesCommand, commandCodec, commandCodec, getBufferProvider(),
                                       provider.getServerDescription(), provider.getChannel(), true).execute();
        } catch (MongoCommandFailureException e) {
            return ignoreNamespaceNotFoundExceptions(e);
        }
    }

    //TODO: work out a way to reuse this
    private CommandResult ignoreNamespaceNotFoundExceptions(final MongoCommandFailureException e) {
        if (!e.getCommandResult().getErrorMessage().equals("ns not found")) {
            throw e;
        }
        return e.getCommandResult();
    }

    private ServerChannelProvider getServerChannelProvider() {
        return getSession().createServerChannelProvider(new ServerChannelProviderOptions(false, new PrimaryServerSelector()));
    }
}
