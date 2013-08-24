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
import org.mongodb.operation.protocol.DeleteCommandProtocol;
import org.mongodb.operation.protocol.DeleteProtocol;
import org.mongodb.operation.protocol.WriteCommandProtocol;
import org.mongodb.operation.protocol.WriteProtocol;
import org.mongodb.session.Session;

import java.util.Arrays;
import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;

public class RemoveOperation extends BaseWriteOperation {
    private final List<Remove> removes;
    private final Encoder<Document> queryEncoder;

    public RemoveOperation(final MongoNamespace namespace, final Remove remove, final Encoder<Document> queryEncoder,
                           final BufferProvider bufferProvider, final Session session, final boolean closeSession) {
        this(namespace, remove.getWriteConcern(), Arrays.asList(remove), queryEncoder, bufferProvider, session, closeSession);
    }

    public RemoveOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final List<Remove> removes,
                           final Encoder<Document> queryEncoder, final BufferProvider bufferProvider, final Session session,
                           final boolean closeSession) {
        super(namespace, writeConcern, bufferProvider, session, closeSession);
        this.removes = notNull("removes", removes);
        this.queryEncoder = notNull("queryEncoder", queryEncoder);
    }

    @Override
    protected WriteProtocol getWriteProtocol(final ServerDescription serverDescription, final Connection connection) {
        return new DeleteProtocol(getNamespace(), getWriteConcern(), removes, queryEncoder, getBufferProvider(), serverDescription,
                connection, false);
    }

    @Override
    protected WriteCommandProtocol getCommandProtocol(final ServerDescription serverDescription, final Connection connection) {
        return new DeleteCommandProtocol(getNamespace(), getWriteConcern(), removes, queryEncoder, getBufferProvider(), serverDescription,
                connection, false);
    }
}
