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
import com.mongodb.client.model.Collation;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;
import static com.mongodb.internal.operation.SyncOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation to create a view.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class CreateViewOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final TimeoutSettings timeoutSettings;
    private final ClientSideOperationTimeout clientSideOperationTimeout;
    private final String databaseName;
    private final String viewName;
    private final String viewOn;
    private final List<BsonDocument> pipeline;
    private final WriteConcern writeConcern;
    private Collation collation;

    public CreateViewOperation(final TimeoutSettings timeoutSettings, final String databaseName,
            final String viewName, final String viewOn, final List<BsonDocument> pipeline, final WriteConcern writeConcern) {
        this.timeoutSettings = timeoutSettings;
        this.clientSideOperationTimeout = new ClientSideOperationTimeout(timeoutSettings);
        this.databaseName = notNull("databaseName", databaseName);
        this.viewName = notNull("viewName", viewName);
        this.viewOn = notNull("viewOn", viewOn);
        this.pipeline = notNull("pipeline", pipeline);
        this.writeConcern = notNull("writeConcern", writeConcern);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Gets the name of the view to create.
     *
     * @return the view name
     */
    public String getViewName() {
        return viewName;
    }

    /**
     * Gets the name of the collection or view that backs this view.
     *
     * @return the name of the collection or view that backs this view
     */
    public String getViewOn() {
        return viewOn;
    }

    /**
     * Gets the pipeline that defines the view.
     *
     * @return the pipeline that defines the view
     */
    public List<BsonDocument> getPipeline() {
        return pipeline;
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets the default collation for the view
     *
     * @return the collation, which may be null
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the default collation for the view.
     *
     * @param collation the collation, which may be null
     * @return this
     */
    public CreateViewOperation collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, connection -> {
            executeCommand(binding, databaseName, getCommand(), new BsonDocumentCodec(),
                    writeConcernErrorTransformer());
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
                SingleResultCallback<Void> wrappedCallback = releasingCallback(errHandlingCallback, connection);
                executeCommandAsync(binding, databaseName, getCommand(), connection, writeConcernErrorTransformerAsync(),
                        wrappedCallback);
            }
        });
    }

    private BsonDocument getCommand() {
        BsonDocument commandDocument = new BsonDocument("create", new BsonString(viewName))
                                               .append("viewOn", new BsonString(viewOn))
                                               .append("pipeline", new BsonArray(pipeline));
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }

        appendWriteConcernToCommand(writeConcern, commandDocument);
        return commandDocument;
    }
}
