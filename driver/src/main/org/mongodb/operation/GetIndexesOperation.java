/*
 * Copyright (c) 2008 MongoDB, Inc.
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
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoQueryCursor;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.List;

import static org.mongodb.ReadPreference.primary;
import static org.mongodb.assertions.Assertions.notNull;

public class GetIndexesOperation extends BaseOperation<List<Document>> {
    private final Encoder<Document> simpleDocumentEncoder = new DocumentCodec();
    private final MongoNamespace indexesNamespace;
    private final Find queryForCollectionNamespace;

    public GetIndexesOperation(final MongoNamespace collectionNamespace,
                               final BufferProvider bufferProvider, final Session session) {
        super(bufferProvider, session, false);
        notNull("collectionNamespace", collectionNamespace);
        this.indexesNamespace = new MongoNamespace(collectionNamespace.getDatabaseName(), "system.indexes");
        this.queryForCollectionNamespace = new Find(new Document("ns", collectionNamespace.getFullName())).readPreference(primary());
    }

    @Override
    public List<Document> execute() {
        List<Document> retVal = new ArrayList<Document>();
        MongoCursor<Document> cursor = new MongoQueryCursor<Document>(indexesNamespace, queryForCollectionNamespace, simpleDocumentEncoder,
                                                                      new DocumentCodec(), getBufferProvider(), getSession(),
                                                                      isCloseSession());
        while (cursor.hasNext()) {
            retVal.add(cursor.next());
        }
        return retVal;
    }
}
