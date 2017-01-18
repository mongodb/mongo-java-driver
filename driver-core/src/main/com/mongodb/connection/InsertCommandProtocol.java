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

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.util.List;

import static java.lang.String.format;

/**
 * An implementation of the insert command.
 *
 * @mongodb.driver.manual reference/command/insert/#dbcmd.insert Insert Command
 */
class InsertCommandProtocol extends WriteCommandProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.insert");

    private final List<InsertRequest> insertRequests;

    InsertCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                          final Boolean bypassDocumentValidation, final List<InsertRequest> insertRequests) {
        super(namespace, ordered, writeConcern, bypassDocumentValidation);
        this.insertRequests = insertRequests;
    }

    @Override
    public BulkWriteResult execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Inserting %d documents into namespace %s on connection [%s] to server %s",
                                insertRequests.size(),
                                getNamespace(),
                                connection.getDescription().getConnectionId(),
                                connection.getDescription().getServerAddress()));
        }
        BulkWriteResult writeResult = super.execute(connection);
        LOGGER.debug("Insert completed");
        return writeResult;
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<BulkWriteResult> callback) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously inserting %d documents into namespace %s on connection [%s] to server %s",
                                    insertRequests.size(), getNamespace(), connection.getDescription().getConnectionId(),
                                    connection.getDescription().getServerAddress()));
            }
            super.executeAsync(connection, new SingleResultCallback<BulkWriteResult>() {
                @Override
                public void onResult(final BulkWriteResult result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        LOGGER.debug("Asynchronous insert completed");
                        callback.onResult(result, null);
                    }
                }
            });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.INSERT;
    }

    @Override
    protected InsertCommandMessage createRequestMessage(final MessageSettings messageSettings) {
        return new InsertCommandMessage(getNamespace(), isOrdered(), getWriteConcern(), getBypassDocumentValidation(), messageSettings,
                insertRequests);
    }

    @Override
    protected com.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }

}
