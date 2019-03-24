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

import com.mongodb.MongoClientException;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.operation.OperationHelper.AsyncCallableWithConnection;
import com.mongodb.internal.operation.OperationHelper.CallableWithConnection;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.List;

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
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotFour;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation to create a view.
 *
 * @since 3.4
 * @mongodb.server.release 3.4
 * @mongodb.driver.manual reference/command/create Create
 */
public class CreateViewOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final String databaseName;
    private final String viewName;
    private final String viewOn;
    private final List<BsonDocument> pipeline;
    private final WriteConcern writeConcern;
    private Collation collation;

    /**
     * Construct a new instance.
     *
     * @param databaseName the name of the database for the operation, which may not be null
     * @param viewName     the name of the collection to be created, which may not be null
     * @param viewOn       the name of the collection or view that backs this view, which may not be null
     * @param pipeline     the aggregation pipeline that defines the view, which may not be null
     * @param writeConcern the write concern, which may not be null
     */
    public CreateViewOperation(final String databaseName, final String viewName, final String viewOn, final List<BsonDocument> pipeline,
                               final WriteConcern writeConcern) {
        this.databaseName = notNull("databaseName", databaseName);
        this.viewName = notNull("viewName", viewName);
        this.viewOn = notNull("viewOn", viewOn);
        this.pipeline = notNull("pipeline", pipeline);
        this.writeConcern = notNull("writeConcern", writeConcern);
    }

    /**
     * Gets the database name
     *
     * @return the database name
     */
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
    public CreateViewOperation collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                if (!serverIsAtLeastVersionThreeDotFour(connection.getDescription())) {
                    throw createExceptionForIncompatibleServerVersion();
                }
                executeCommand(binding, databaseName, getCommand(connection.getDescription()), writeConcernErrorTransformer());
                return null;
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                }
                else {
                    SingleResultCallback<Void> wrappedCallback = releasingCallback(errHandlingCallback, connection);
                    if (!serverIsAtLeastVersionThreeDotFour(connection.getDescription())) {
                        wrappedCallback.onResult(null, createExceptionForIncompatibleServerVersion());
                    }
                    executeCommandAsync(binding, databaseName, getCommand(connection.getDescription()),
                            connection, writeConcernErrorWriteTransformer(), wrappedCallback);
                }
            }
        });
    }

    private BsonDocument getCommand(final ConnectionDescription description) {
        BsonDocument commandDocument = new BsonDocument("create", new BsonString(viewName))
                                               .append("viewOn", new BsonString(viewOn))
                                               .append("pipeline", new BsonArray(pipeline));
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }

        appendWriteConcernToCommand(writeConcern, commandDocument, description);
        return commandDocument;
    }

    private MongoClientException createExceptionForIncompatibleServerVersion() {
        return new MongoClientException("Can not create view.  The minimum server version that supports view creation is 3.4");
    }
}
