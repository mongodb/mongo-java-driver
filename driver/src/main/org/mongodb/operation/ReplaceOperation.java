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
import org.mongodb.operation.protocol.ReplaceProtocol;
import org.mongodb.session.Session;

import static org.mongodb.assertions.Assertions.notNull;

public class ReplaceOperation<T> extends WriteOperationBase {
    private final Replace<T> replace;
    private final Encoder<Document> queryEncoder;
    private final Encoder<T> encoder;

    public ReplaceOperation(final MongoNamespace namespace, final Replace<T> replace, final Encoder<Document> queryEncoder,
                            final Encoder<T> encoder, final BufferProvider bufferProvider, final Session session,
                            final boolean closeSession) {
        super(namespace, replace.getWriteConcern(), bufferProvider, session, closeSession);
        this.replace = notNull("replace", replace);
        this.queryEncoder = notNull("queryEncoder", queryEncoder);
        this.encoder = notNull("encoder", encoder);
    }

    @Override
    protected ReplaceProtocol<T> getProtocol(final ServerConnectionProvider provider) {
        return new ReplaceProtocol<T>(getNamespace(), replace, queryEncoder, encoder, getBufferProvider(),
                provider.getServerDescription(), provider.getConnection(), true);
    }
}
