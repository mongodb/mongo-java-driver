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

import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.protocol.InsertProtocol;
import org.mongodb.session.Session;

import static org.mongodb.assertions.Assertions.notNull;

public class InsertOperation<T> extends WriteOperationBase {
    private final Insert<T> insert;
    private final Encoder<T> encoder;

    public InsertOperation(final MongoNamespace namespace, final Insert<T> insert, final Encoder<T> encoder,
                           final BufferProvider bufferProvider, final Session session, final boolean closeSession) {
        super(namespace, insert.getWriteConcern(), bufferProvider, session, closeSession);
        this.insert = notNull("insert", insert);
        this.encoder = notNull("encoder", encoder);
    }

    @Override
    protected InsertProtocol<T> getProtocol(final ServerConnectionProvider provider) {
        return new InsertProtocol<T>(getNamespace(), insert, encoder, getBufferProvider(), provider.getServerDescription(),
                provider.getConnection(), true);
    }
}
