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
import org.mongodb.WriteConcern;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.protocol.UpdateCommandProtocol;
import org.mongodb.operation.protocol.UpdateProtocol;
import org.mongodb.operation.protocol.WriteCommandProtocol;
import org.mongodb.operation.protocol.WriteProtocol;
import org.mongodb.session.Session;

import java.util.Arrays;
import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;

public class UpdateOperation extends BaseWriteOperation {
    private final List<Update> updates;
    private final Encoder<Document> queryEncoder;

    public UpdateOperation(final MongoNamespace namespace, final Update update, final Encoder<Document> queryEncoder,
                           final BufferProvider bufferProvider, final Session session,
                           final boolean closeSession) {
        this(namespace, update.getWriteConcern(), Arrays.asList(update), queryEncoder, bufferProvider, session, closeSession);
    }

    public UpdateOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final List<Update> updates,
                           final Encoder<Document> queryEncoder, final BufferProvider bufferProvider, final Session session,
                           final boolean closeSession) {
        super(namespace, writeConcern, bufferProvider, session, closeSession);
        this.updates = notNull("update", updates);
        this.queryEncoder = notNull("queryEncoder", queryEncoder);
    }

    @Override
    protected WriteProtocol getWriteProtocol(final ServerDescription serverDescription, final Connection connection) {
        return new UpdateProtocol(getNamespace(), getWriteConcern(), updates, queryEncoder, getBufferProvider(), serverDescription,
                connection, false);
    }

    @Override
    protected WriteCommandProtocol getCommandProtocol(final ServerDescription serverDescription, final Connection connection) {
        return new UpdateCommandProtocol(getNamespace(), getWriteConcern(), updates, queryEncoder, getBufferProvider(), serverDescription,
                connection, false);
    }
}
