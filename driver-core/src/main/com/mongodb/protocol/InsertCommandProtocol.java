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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.connection.Connection;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.WriteRequest;
import com.mongodb.protocol.message.InsertCommandMessage;
import com.mongodb.protocol.message.MessageSettings;
import org.mongodb.BulkWriteResult;

import java.util.List;

import static java.lang.String.format;

/**
 * An implementation of the insert command.
 *
 * @since 3.0
 * @mongodb.driver.manual manual/reference/command/insert/#dbcmd.insert Insert Command
 */
public class InsertCommandProtocol extends WriteCommandProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.insert");

    private final List<InsertRequest> insertRequests;

    /**
     * Construct an instance.
     *
     * @param namespace the namespace
     * @param ordered whether the inserts are ordered
     * @param writeConcern the write concern
     * @param insertRequests the list of inserts
     */
    public InsertCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                 final List<InsertRequest> insertRequests) {
        super(namespace, ordered, writeConcern);
        this.insertRequests = insertRequests;
    }

    @Override
    public BulkWriteResult execute(final Connection connection) {
        LOGGER.debug(format("Inserting %d documents into namespace %s on connection [%s] to server %s", insertRequests.size(),
                            getNamespace(), connection.getId(), connection.getServerAddress()));
        BulkWriteResult writeResult = super.execute(connection);
        LOGGER.debug("Insert completed");
        return writeResult;
    }

    @Override
    public MongoFuture<BulkWriteResult> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously inserting %d documents into namespace %s on connection [%s] to server %s",
                            insertRequests.size(), getNamespace(), connection.getId(), connection.getServerAddress()));
        final SingleResultFuture<BulkWriteResult> future = new SingleResultFuture<BulkWriteResult>();
        super.executeAsync(connection).register(new SingleResultCallback<BulkWriteResult>() {
            @Override
            public void onResult(final BulkWriteResult result, final MongoException e) {
                if (e != null) {
                    LOGGER.debug("Asynchronous insert completed");
                }
                future.init(result, e);
            }
        });
        return future;
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.INSERT;
    }

    @Override
    protected InsertCommandMessage createRequestMessage(final MessageSettings messageSettings) {
        return new InsertCommandMessage(getNamespace(), isOrdered(), getWriteConcern(), insertRequests, messageSettings);
    }

    @Override
    protected com.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }

}
