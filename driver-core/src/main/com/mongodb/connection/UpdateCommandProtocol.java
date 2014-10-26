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

package com.mongodb.connection;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.operation.UpdateRequest;
import com.mongodb.operation.WriteRequest;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * An implementation of the update command.
 *
 * @mongodb.driver.manual reference/command/insert/#dbcmd.update Update Command
 */
class UpdateCommandProtocol extends WriteCommandProtocol {

    private static final com.mongodb.diagnostics.logging.Logger LOGGER = Loggers.getLogger("protocol.update");

    private final List<UpdateRequest> updates;

    /**
     * Construct an instance.
     *
     * @param namespace    the namespace
     * @param ordered      whether the inserts are ordered
     * @param writeConcern the write concern
     * @param updates      the list of updates
     */
    public UpdateCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                 final List<UpdateRequest> updates) {
        super(namespace, ordered, writeConcern);
        this.updates = notNull("update", updates);
    }

    @Override
    public BulkWriteResult execute(final InternalConnection connection) {
        LOGGER.debug(format("Updating documents in namespace %s on connection [%s] to server %s", getNamespace(),
                            connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
        BulkWriteResult writeResult = super.execute(connection);
        LOGGER.debug("Update completed");
        return writeResult;
    }

    @Override
    public MongoFuture<BulkWriteResult> executeAsync(final InternalConnection connection) {
        LOGGER.debug(format("Asynchronously updating documents in namespace %s on connection [%s] to server %s", getNamespace(),
                            connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
        final SingleResultFuture<BulkWriteResult> future = new SingleResultFuture<BulkWriteResult>();
        super.executeAsync(connection).register(new SingleResultCallback<BulkWriteResult>() {
            @Override
            public void onResult(final BulkWriteResult result, final MongoException e) {
                if (e != null) {
                    LOGGER.debug("Asynchronous update completed");
                }
                future.init(result, e);
            }
        });
        return future;
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.UPDATE;
    }

    @Override
    protected UpdateCommandMessage createRequestMessage(final MessageSettings messageSettings) {
        return new UpdateCommandMessage(getNamespace(), isOrdered(), getWriteConcern(), updates, messageSettings);
    }

    @Override
    protected com.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }

}
