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

import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.Operation;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.operation.protocol.UpdateProtocolOperation;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

public class UpdateOperation implements Operation<CommandResult> {
    private final MongoNamespace namespace;
    private final Update update;
    private final Encoder<Document> queryEncoder;
    private final BufferProvider bufferProvider;
    private final Session session;
    private final boolean closeSession;

    public UpdateOperation(final MongoNamespace namespace, final Update update, final Encoder<Document> queryEncoder,
                           final BufferProvider bufferProvider, final Session session,
                           final boolean closeSession) {
        this.namespace = namespace;
        this.update = update;
        this.queryEncoder = queryEncoder;
        this.bufferProvider = bufferProvider;
        this.session = session;
        this.closeSession = closeSession;
    }

    @Override
    public CommandResult execute() {
        ServerConnectionProvider provider = session.createServerConnectionProvider(
                new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));
        Connection connection = provider.getConnection();
        try {
            return new UpdateProtocolOperation(namespace, update, queryEncoder, bufferProvider, provider.getServerDescription(),
                    provider.getConnection(), true).execute();
        } finally {
            connection.close();
            if (closeSession) {
                session.close();
            }
        }
    }
}
