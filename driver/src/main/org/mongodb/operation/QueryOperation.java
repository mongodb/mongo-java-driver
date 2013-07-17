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

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoQueryCursor;
import org.mongodb.Operation;
import org.mongodb.connection.BufferProvider;
import org.mongodb.session.Session;

public class QueryOperation<T> implements Operation<MongoCursor<T>> {
    private final Find find;
    private final Encoder<Document> queryEncoder;
    private final Decoder<T> resultDecoder;
    private final Session session;
    private final boolean closeSession;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public QueryOperation(final MongoNamespace namespace, final Find find, final Encoder<Document> queryEncoder,
                          final Decoder<T> resultDecoder, final BufferProvider bufferProvider,
                          final Session session, final boolean closeSession) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.find = find;
        this.queryEncoder = queryEncoder;
        this.resultDecoder = resultDecoder;
        this.session = session;
        this.closeSession = closeSession;
    }

    @Override
    public MongoCursor<T> execute() {
         return new MongoQueryCursor<T>(namespace, find, queryEncoder, resultDecoder, bufferProvider, session, closeSession);
    }
}
