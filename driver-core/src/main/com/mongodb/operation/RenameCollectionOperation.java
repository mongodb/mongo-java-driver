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

package com.mongodb.operation;

import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.MongoNamespace.asNamespaceString;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.VoidTransformer;

/**
 * An operation that renames the given collection to the new name.  If the new name is the same as an existing collection and dropTarget is
 * true, this existing collection will be dropped. If dropTarget is false and the newCollectionName is the same as an existing collection, a
 * MongoServerException will be thrown.
 *
 * @since 3.0
 */
public class RenameCollectionOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final String databaseName;
    private final String originalCollectionName;
    private final String newCollectionName;
    private final boolean dropTarget;

    /**
     * @param databaseName the name of the database for the operation.
     * @param originalCollectionName the name of the collection to rename
     * @param newCollectionName      the desired new name for the collection
     * @param dropTarget             set to true if you want any existing database with newCollectionName to be dropped during the rename
     */
    public RenameCollectionOperation(final String databaseName, final String originalCollectionName, final String newCollectionName,
                                     final boolean dropTarget) {
        super();
        this.originalCollectionName = notNull("originalCollectionName", originalCollectionName);
        this.newCollectionName = notNull("newCollectionName", newCollectionName);
        this.dropTarget = notNull("dropTarget", dropTarget);
        this.databaseName = notNull("databaseName", databaseName);
    }

    /**
     * Rename the collection with {@code oldCollectionName} in database {@code databaseName} to the {@code newCollectionName}.
     *
     * @param binding the binding
     * @return a void result
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection and dropTarget
     *                                          is false, or if the oldCollectionName is the name of a collection that doesn't exist
     */
    @Override
    public Void execute(final WriteBinding binding) {
        return executeWrappedCommandProtocol("admin", getCommand(), binding, new VoidTransformer<BsonDocument>());
    }

    /**
     * Rename the collection with {@code oldCollectionName} in database {@code databaseName} to the {@code newCollectionName}.
     *
     * @param binding the binding
     * @return a future for a void result
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection and dropTarget
     *                                          is false, or if the oldCollectionName is the name of a collection that doesn't exist
     */
    @Override
    public MongoFuture<Void> executeAsync(final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync("admin", getCommand(), binding, new VoidTransformer<BsonDocument>());
    }

    private BsonDocument getCommand() {
        return new BsonDocument("renameCollection", new BsonString(asNamespaceString(databaseName, originalCollectionName)))
               .append("to", new BsonString(asNamespaceString(databaseName, newCollectionName)))
               .append("dropTarget", BsonBoolean.valueOf(dropTarget));
    }

}
