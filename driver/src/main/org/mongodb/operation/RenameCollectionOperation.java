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
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import static org.mongodb.MongoNamespace.asNamespaceString;

public class RenameCollectionOperation extends BaseOperation<CommandResult> {
    private final Codec<Document> commandCodec = new DocumentCodec();
    private final String originalCollectionName;
    private final String newCollectionName;
    private final boolean dropTarget;
    private final String databaseName;

    public RenameCollectionOperation(final BufferProvider bufferProvider, final Session session, final boolean closeSession,
                                     final String databaseName, final String originalCollectionName, final String newCollectionName,
                                     final boolean dropTarget) {
        super(bufferProvider, session, closeSession);
        this.originalCollectionName = originalCollectionName;
        this.newCollectionName = newCollectionName;
        this.dropTarget = dropTarget;
        this.databaseName = databaseName;
    }

    @Override
    public CommandResult execute() {
        ServerConnectionProvider provider = createServerConnectionProvider();
        return new CommandProtocol("admin", createCommand(), commandCodec, commandCodec,
                                   getBufferProvider(), provider.getServerDescription(), provider.getConnection(), false)
                   .execute();
    }

    private Document createCommand() {
        return new Document("renameCollection", asNamespaceString(databaseName, originalCollectionName))
                   .append("to", asNamespaceString(databaseName, newCollectionName))
                   .append("dropTarget", dropTarget);
    }

    private ServerConnectionProvider createServerConnectionProvider() {
        //TODO: otra vez?
        return getSession().createServerConnectionProvider(new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));
    }
}
