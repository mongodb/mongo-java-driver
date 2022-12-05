/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorWriteTransformer;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.releasingCallback;
import static com.mongodb.internal.operation.OperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.withConnection;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation that renames the given collection to the new name.
 *
 * <p>If the new name is the same as an existing collection and dropTarget is true, this existing collection will be dropped. If
 * dropTarget is false and the newCollectionName is the same as an existing collection, a MongoServerException will be thrown.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class RenameCollectionOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace originalNamespace;
    private final MongoNamespace newNamespace;
    private final WriteConcern writeConcern;
    private boolean dropTarget;

    public RenameCollectionOperation(final MongoNamespace originalNamespace, final MongoNamespace newNamespace) {
        this(originalNamespace, newNamespace, null);
    }

    public RenameCollectionOperation(final MongoNamespace originalNamespace, final MongoNamespace newNamespace,
                                     @Nullable final WriteConcern writeConcern) {
        this.originalNamespace = notNull("originalNamespace", originalNamespace);
        this.newNamespace = notNull("newNamespace", newNamespace);
        this.writeConcern = writeConcern;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public boolean isDropTarget() {
        return dropTarget;
    }

    public RenameCollectionOperation dropTarget(final boolean dropTarget) {
        this.dropTarget = dropTarget;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, connection -> executeCommand(binding, "admin", getCommand(), connection, writeConcernErrorTransformer()));
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(binding, (connection, t) -> {
            SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (t != null) {
                errHandlingCallback.onResult(null, t);
            } else {
                executeCommandAsync(binding, "admin", getCommand(), assertNotNull(connection),
                        writeConcernErrorWriteTransformer(), releasingCallback(errHandlingCallback, connection));
            }
        });
    }

    private BsonDocument getCommand() {
        BsonDocument commandDocument = new BsonDocument("renameCollection", new BsonString(originalNamespace.getFullName()))
                                            .append("to", new BsonString(newNamespace.getFullName()))
                                            .append("dropTarget", BsonBoolean.valueOf(dropTarget));
        appendWriteConcernToCommand(writeConcern, commandDocument);
        return commandDocument;
    }
}
