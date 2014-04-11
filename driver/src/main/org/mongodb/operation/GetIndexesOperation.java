/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.OperationHelper.executeProtocol;
import static org.mongodb.operation.OperationHelper.getPrimaryConnectionProvider;

public class GetIndexesOperation implements Operation<List<Document>> {
    private final MongoNamespace collectionNamespace;

    public GetIndexesOperation(final MongoNamespace collectionNamespace) {
        this.collectionNamespace = notNull("collectionNamespace", collectionNamespace);
    }

    @Override
    public List<Document> execute(final Session session) {
        QueryResult<Document> queryResult =
        executeProtocol(new QueryProtocol<Document>(getIndexNamespace(), EnumSet.noneOf(QueryFlag.class), 0, 0, asQueryDocument(), null,
                                                    new DocumentCodec(), new DocumentCodec()),
                        session);
        MongoCursor<Document> cursor = new MongoQueryCursor<Document>(getIndexNamespace(), queryResult, 0, 0,
                                                                      new DocumentCodec(),
                                                                      getPrimaryConnectionProvider(session));
        try {
            List<Document> retVal = new ArrayList<Document>();
            while (cursor.hasNext()) {
                retVal.add(cursor.next());
            }
            return retVal;
        } finally {
            cursor.close();
        }
    }

    private Document asQueryDocument() {
        return new Document("ns", collectionNamespace.getFullName());
    }

    private MongoNamespace getIndexNamespace() {
        return new MongoNamespace(collectionNamespace.getDatabaseName(), "system.indexes");
    }
}
