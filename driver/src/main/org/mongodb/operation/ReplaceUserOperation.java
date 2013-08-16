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
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.protocol.ReplaceProtocol;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import java.util.Arrays;

public class ReplaceUserOperation extends OperationBase<CommandResult> {
    private final String database;
    private final Document userDocument;

    public ReplaceUserOperation(final String database, final Document userDocument, final BufferProvider bufferProvider,
                                final Session session, final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.database = database;
        this.userDocument = userDocument;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CommandResult execute() {
        ServerConnectionProvider serverConnectionProvider = getSession().createServerConnectionProvider(
                new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));
        MongoNamespace namespace = new MongoNamespace(database, "system.users");
        DocumentCodec codec = new DocumentCodec();
        return new ReplaceProtocol<Document>(namespace, WriteConcern.ACKNOWLEDGED,
                Arrays.asList(new Replace<Document>(WriteConcern.ACKNOWLEDGED, new Document("_id", userDocument.get("_id")),
                        userDocument)), codec, codec, getBufferProvider(), serverConnectionProvider.getServerDescription(),
                serverConnectionProvider.getConnection(), true).execute();
    }
}