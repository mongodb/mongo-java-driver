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

package com.mongodb.internal.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ByteBufferBsonOutput;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.bulk.DeleteRequest;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import static com.mongodb.internal.connection.ByteBufBsonDocument.createOne;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

/**
 * An implementation of the MongoDB OP_DELETE wire protocol.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-delete OP_DELETE
 */
class DeleteProtocol extends WriteProtocol {
    private static final Logger LOGGER = Loggers.getLogger("protocol.delete");

    private final DeleteRequest deleteRequest;

    DeleteProtocol(final MongoNamespace namespace, final boolean ordered, final DeleteRequest deleteRequest) {
        super(namespace, ordered);
        this.deleteRequest = deleteRequest;
    }

    @Override
    public WriteConcernResult execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Deleting documents from namespace %s on connection [%s] to server %s", getNamespace(),
                                connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
        }
        WriteConcernResult writeConcernResult = super.execute(connection);
        LOGGER.debug("Delete completed");
        return writeConcernResult;
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<WriteConcernResult> callback) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously deleting documents in namespace %s on connection [%s] to server %s", getNamespace(),
                                    connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
            }
            super.executeAsync(connection, new SingleResultCallback<WriteConcernResult>() {
                @Override
                public void onResult(final WriteConcernResult result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        LOGGER.debug("Asynchronous delete completed");
                        callback.onResult(result, null);
                    }
                }
            });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    @Override
    protected BsonDocument getAsWriteCommand(final ByteBufferBsonOutput bsonOutput, final int firstDocumentPosition) {
        BsonDocument deleteDocument = new BsonDocument("q", createOne(bsonOutput, firstDocumentPosition))
                                      .append("limit", deleteRequest.isMulti() ? new BsonInt32(0) : new BsonInt32(1));
        return getBaseCommandDocument("delete").append("deletes", new BsonArray(singletonList(deleteDocument)));
    }

    @Override
    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new DeleteMessage(getNamespace().getFullName(), deleteRequest, settings);
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}
