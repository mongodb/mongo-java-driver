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

import com.mongodb.WriteConcern;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

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
 * Operation to drop a database in MongoDB.  The {@code execute} method throws MongoCommandFailureException if something goes wrong, but
 * it will not throw an Exception if the collection does not exist before trying to drop it.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class DropDatabaseOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final String databaseName;
    private final WriteConcern writeConcern;

    public DropDatabaseOperation(final String databaseName) {
        this(databaseName, null);
    }

    public DropDatabaseOperation(final String databaseName, @Nullable final WriteConcern writeConcern) {
        this.databaseName = notNull("databaseName", databaseName);
        this.writeConcern = writeConcern;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, connection -> {
            executeCommand(binding, databaseName, getCommand(), connection, writeConcernErrorTransformer());
            return null;
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(binding, (connection, t) -> {
            SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (t != null) {
                errHandlingCallback.onResult(null, t);
            } else {
                executeCommandAsync(binding, databaseName, getCommand(), connection,
                        writeConcernErrorWriteTransformer(), releasingCallback(errHandlingCallback, connection));

            }
        });
    }

    private BsonDocument getCommand() {
        BsonDocument commandDocument = new BsonDocument("dropDatabase", new BsonInt32(1));
        appendWriteConcernToCommand(writeConcern, commandDocument);
        return commandDocument;
    }
}
