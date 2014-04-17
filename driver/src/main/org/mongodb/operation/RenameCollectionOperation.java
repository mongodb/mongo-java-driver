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
import org.mongodb.session.Session;

import static org.mongodb.MongoNamespace.asNamespaceString;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.ignoreResult;

/**
 * An operation that renames the given collection to the new name.  If the new name is the same as an existing collection and
 * dropTarget is true, this existing collection will be dropped. If dropTarget is false and the newCollectionName is the same as an existing
 * collection, a MongoServerException will be thrown.
 *
 * @3.0
 */
public class RenameCollectionOperation implements AsyncOperation<Void>, Operation<Void> {
    private final String originalCollectionName;
    private final String newCollectionName;
    private final boolean dropTarget;
    private final String databaseName;

    /**
     * @param databaseName           the name of the database containing the collection to rename
     * @param originalCollectionName the name of the collection to rename
     * @param newCollectionName      the desired new name for the collection
     * @param dropTarget             set to true if you want any existing database with newCollectionName to be dropped during the rename
     */
    public RenameCollectionOperation(final String databaseName, final String originalCollectionName, final String newCollectionName,
                                     final boolean dropTarget) {
        super();
        this.originalCollectionName = originalCollectionName;
        this.newCollectionName = newCollectionName;
        this.dropTarget = dropTarget;
        this.databaseName = databaseName;
    }

    /**
     * Rename the collection with {@code oldCollectionName} in database {@code databaseName} to the {@code newCollectionName}.
     *
     * @throws org.mongodb.MongoServerException
     *          if you provide a newCollectionName that is the name of an existing collection and dropTarget is false,
     *          or if the oldCollectionName is the name of a collection that doesn't exist
     * @param session
     */
    @Override
    public Void execute(final Session session) {
        executeWrappedCommandProtocol("admin", getCommand(), session);
        return null;
    }

    /**
     * Rename the collection with {@code oldCollectionName} in database {@code databaseName} to the {@code newCollectionName}.
     *
     * @throws org.mongodb.MongoServerException
     *          if you provide a newCollectionName that is the name of an existing collection and dropTarget is false,
     *          or if the oldCollectionName is the name of a collection that doesn't exist
     * @param session
     */
    @Override
    public MongoFuture<Void> executeAsync(final Session session) {
        return ignoreResult(executeWrappedCommandProtocolAsync("admin", getCommand(), session));
    }

    private Document getCommand() {
        return new Document("renameCollection", asNamespaceString(databaseName, originalCollectionName))
                   .append("to", asNamespaceString(databaseName, newCollectionName))
                   .append("dropTarget", dropTarget);
    }

}
