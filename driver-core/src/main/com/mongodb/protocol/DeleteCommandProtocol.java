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

package com.mongodb.protocol;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.MongoFuture;
import com.mongodb.connection.Connection;
import com.mongodb.connection.SingleResultCallback;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.operation.RemoveRequest;
import com.mongodb.operation.SingleResultFuture;
import com.mongodb.operation.WriteRequest;
import com.mongodb.protocol.message.DeleteCommandMessage;
import com.mongodb.protocol.message.MessageSettings;
import org.mongodb.BulkWriteResult;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

public class DeleteCommandProtocol extends WriteCommandProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.delete");

    private final List<RemoveRequest> removeRequests;

    public DeleteCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                 final List<RemoveRequest> removeRequests) {
        super(namespace, ordered, writeConcern);
        this.removeRequests = notNull("removes", removeRequests);
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
        return WriteRequest.Type.REMOVE;
    }

    @Override
    protected DeleteCommandMessage createRequestMessage(final MessageSettings messageSettings) {
        return new DeleteCommandMessage(getNamespace(), isOrdered(), getWriteConcern(), removeRequests, messageSettings);
    }

    @Override
    protected com.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }

}
