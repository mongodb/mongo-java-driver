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
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.Session;

import java.util.EnumSet;
import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.OperationHelper.executeProtocol;
import static org.mongodb.operation.OperationHelper.executeProtocolAsync;
import static org.mongodb.operation.OperationHelper.queryResultToList;
import static org.mongodb.operation.OperationHelper.queryResultToListAsync;

public class GetCollectionNamesOperation implements AsyncOperation<List<String>>, Operation<List<String>> {
    private final String databaseName;

    public GetCollectionNamesOperation(final String databaseName) {
        this.databaseName = notNull("databaseName", databaseName);
    }

    @Override
    public List<String> execute(final Session session) {
        final QueryResult<Document> queryResult = executeProtocol(getProtocol(), session);
        return queryResultToList(queryResult, session, getNamespace(), new DocumentCodec(), transformer());
    }

    @Override
    public MongoFuture<List<String>> executeAsync(final Session session) {
        final MongoFuture<QueryResult<Document>> queryResult = executeProtocolAsync(getProtocol(), session);
        return queryResultToListAsync(queryResult, session, getNamespace(), new DocumentCodec(), transformer());
    }

    private TransformBlock<Document, String> transformer() {
        return new TransformBlock<Document, String>() {
            @Override
            public String apply(final Document document) {
                String collectionName = document.getString("name");
                if (!collectionName.contains("$")) {
                    return collectionName.substring(databaseName.length() + 1);
                }
                return null;
            }
        };
    }

    private MongoNamespace getNamespace() {
        return new MongoNamespace(databaseName, "system.namespaces");
    }

    private QueryProtocol<Document> getProtocol() {
        return new QueryProtocol<Document>(getNamespace(), EnumSet.noneOf(QueryFlag.class), 0, 0, new Document(), null,
                new DocumentCodec(), new DocumentCodec());
    }

}
