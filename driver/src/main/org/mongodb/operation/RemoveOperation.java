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
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.protocol.RemoveProtocol;
import org.mongodb.session.Session;

import static org.mongodb.assertions.Assertions.notNull;

public class RemoveOperation extends WriteOperationBase {
    private final Remove remove;
    private final Encoder<Document> queryEncoder;

    public RemoveOperation(final MongoNamespace namespace, final Remove remove, final Encoder<Document> queryEncoder,
                           final BufferProvider bufferProvider, final Session session, final boolean closeSession) {
        super(namespace, remove.getWriteConcern(), bufferProvider, session, closeSession);
        this.remove = notNull("remove", remove);
        this.queryEncoder = notNull("queryEncoder", queryEncoder);
    }

    @Override
    protected RemoveProtocol getProtocol(final ServerConnectionProvider provider) {
        return new RemoveProtocol(getNamespace(), remove, queryEncoder, getBufferProvider(), provider.getServerDescription(),
                provider.getConnection(), true);
    }
}
