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
import org.mongodb.operation.protocol.ReplaceCommandProtocol;
import org.mongodb.operation.protocol.ReplaceProtocol;
import org.mongodb.operation.protocol.WriteCommandProtocol;
import org.mongodb.operation.protocol.WriteProtocol;
import org.mongodb.session.Session;

import java.util.Arrays;
import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;

public class ReplaceOperation<T> extends BaseWriteOperation {
    private final List<Replace<T>> replaces;
    private final Encoder<Document> queryEncoder;
    private final Encoder<T> encoder;

    @SuppressWarnings("unchecked")
    public ReplaceOperation(final MongoNamespace namespace, final Replace<T> replace, final Encoder<Document> queryEncoder,
                            final Encoder<T> encoder, final BufferProvider bufferProvider, final Session session,
                            final boolean closeSession) {
        this(namespace, replace.getWriteConcern(), Arrays.asList(replace), queryEncoder, encoder, bufferProvider, session, closeSession);
    }

    public ReplaceOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final List<Replace<T>> replaces,
                            final Encoder<Document> queryEncoder, final Encoder<T> encoder, final BufferProvider bufferProvider,
                            final Session session, final boolean closeSession) {
        super(namespace, writeConcern, bufferProvider, session, closeSession);
        this.replaces = notNull("replace", replaces);
        this.queryEncoder = notNull("queryEncoder", queryEncoder);
        this.encoder = notNull("encoder", encoder);
    }

    @Override
    protected WriteProtocol getWriteProtocol(final ServerDescription serverDescription, final Connection connection) {
        return new ReplaceProtocol<T>(getNamespace(), getWriteConcern(), replaces, queryEncoder, encoder, getBufferProvider(),
                serverDescription, connection, false); }

    @Override
    protected WriteCommandProtocol getCommandProtocol(final ServerDescription serverDescription, final Connection connection) {
        return new ReplaceCommandProtocol<T>(getNamespace(), getWriteConcern(), replaces, queryEncoder, encoder, getBufferProvider(),
                serverDescription, connection, false);
    }
}
