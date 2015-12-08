/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;

import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

/**
 * An implementation of the MongoDB OP_UPDATE wire protocol.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-update OP_UPDATE
 */
class UpdateProtocol extends WriteProtocol {
    private static final com.mongodb.diagnostics.logging.Logger LOGGER = Loggers.getLogger("protocol.update");

    private final List<UpdateRequest> updates;

    /**
     * Construct an instance.
     *
     * @param namespace    the namespace
     * @param ordered      whether the delete are ordered
     * @param writeConcern the write concern to apply
     * @param updates      the updates
     */
    public UpdateProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                          final List<UpdateRequest> updates) {
        super(namespace, ordered, writeConcern);
        this.updates = updates;
    }

    @Override
    public WriteConcernResult execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Updating documents in namespace %s on connection [%s] to server %s", getNamespace(),
                                connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
        }
        WriteConcernResult writeConcernResult = super.execute(connection);
        LOGGER.debug("Update completed");
        return writeConcernResult;
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<WriteConcernResult> callback) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously updating documents in namespace %s on connection [%s] to server %s", getNamespace(),
                                    connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
            }
            super.executeAsync(connection, new SingleResultCallback<WriteConcernResult>() {
                @Override
                public void onResult(final WriteConcernResult result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        LOGGER.debug("Asynchronous update completed");
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
        List<ByteBufBsonDocument> documents = ByteBufBsonDocument.create(bsonOutput, firstDocumentPosition);
        BsonDocument updateDocument = new BsonDocument("q", documents.get(0)).append("u", documents.get(1));
        if (updates.get(0).isMulti()) {
            updateDocument.append("multi", BsonBoolean.TRUE);
        }
        if (updates.get(0).isUpsert()) {
            updateDocument.append("upsert", BsonBoolean.TRUE);
        }
        return getBaseCommandDocument("update").append("updates", new BsonArray(singletonList(updateDocument)));
    }


    @Override
    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new UpdateMessage(getNamespace().getFullName(), updates, settings);
    }

    @Override
    protected void appendToWriteCommandResponseDocument(final RequestMessage curMessage, final RequestMessage nextMessage,
                                                        final WriteConcernResult writeConcernResult, final BsonDocument response) {
        response.append("n", new BsonInt32(writeConcernResult.getCount()));

        UpdateMessage updateMessage = (UpdateMessage) curMessage;
        UpdateRequest updateRequest = updateMessage.getUpdateRequests().get(0);
        BsonValue upsertedId = null;
        if (writeConcernResult.getUpsertedId() != null) {
            upsertedId = writeConcernResult.getUpsertedId();
        } else if (!writeConcernResult.isUpdateOfExisting() && updateRequest.isUpsert()) {
            if (updateRequest.getUpdate().containsKey("_id")) {
                upsertedId = updateRequest.getUpdate().get("_id");
            } else if (updateRequest.getFilter().containsKey("_id")) {
                upsertedId = updateRequest.getFilter().get("_id");
            }
        }
        if (upsertedId != null) {
            response.append("upserted", new BsonArray(singletonList(new BsonDocument("index", new BsonInt32(0))
                                                                    .append("_id", upsertedId))));
        }
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

}
