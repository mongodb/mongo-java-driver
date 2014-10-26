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
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.operation.DeleteRequest;
import com.mongodb.operation.WriteRequest;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * An implementation of the delete command.
 *
 * @mongodb.driver.manual reference/command/insert/#dbcmd.delete Delete Command
 * @since 3.0
 */
public class DeleteCommandProtocol extends WriteCommandProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.delete");

    private final List<DeleteRequest> deleteRequests;

    /**
     * Construct an instance.
     *
     * @param namespace    the namespace
     * @param ordered      whether the inserts are ordered
     * @param writeConcern the write concern
     * @param deletes      the list of deletes
     */
    public DeleteCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                 final List<DeleteRequest> deletes) {
        super(namespace, ordered, writeConcern);
        this.deleteRequests = notNull("removes", deletes);
    }

    @Override
    public BulkWriteResult execute(final Connection connection) {
        LOGGER.debug(format("Deleting documents from namespace %s on connection [%s] to server %s", getNamespace(),
                            connection.getId(), connection.getServerAddress()));
        BulkWriteResult writeResult = super.execute(connection);
        LOGGER.debug("Delete completed");
        return writeResult;
    }

    @Override
    public MongoFuture<BulkWriteResult> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously deleting documents from namespace %s on connection [%s] to server %s", getNamespace(),
                            connection.getId(), connection.getServerAddress()));
        final SingleResultFuture<BulkWriteResult> future = new SingleResultFuture<BulkWriteResult>();
        super.executeAsync(connection).register(new SingleResultCallback<BulkWriteResult>() {
            @Override
            public void onResult(final BulkWriteResult result, final MongoException e) {
                if (e != null) {
                    LOGGER.debug("Asynchronous delete completed");
                }
                future.init(result, e);
            }
        });
        return future;
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.DELETE;
    }

    @Override
    protected DeleteCommandMessage createRequestMessage(final MessageSettings messageSettings) {
        return new DeleteCommandMessage(getNamespace(), isOrdered(), getWriteConcern(), deleteRequests, messageSettings);
    }

    @Override
    protected com.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }

}
