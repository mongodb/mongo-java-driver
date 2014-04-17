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
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.session.Session;

import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.ignoreNameSpaceErrors;
import static org.mongodb.operation.OperationHelper.ignoreResult;

public class DropIndexOperation implements AsyncOperation<Void>, Operation<Void> {
    private final MongoNamespace namespace;
    private final String indexName;

    public DropIndexOperation(final MongoNamespace namespace, final String indexName) {
        this.namespace = namespace;
        this.indexName = indexName;
    }

    @Override
    public Void execute(final Session session) {
        try {
            executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), session);
        } catch (MongoCommandFailureException e) {
            ignoreNameSpaceErrors(e);
        }
        return null;
    }

    @Override
    public MongoFuture<Void> executeAsync(final Session session) {
        return ignoreResult(ignoreNameSpaceErrors(executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), session)));
    }

    private Document getCommand() {
        return new Document("dropIndexes", namespace.getCollectionName()).append("index", indexName);
    }
}
