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
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.BsonArray;
import org.bson.BsonDocument;

import static java.lang.String.format;

/**
 * An implementation of the insert wire protocol.  This class also takes care of applying the write concern.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-insert OP_INSERT
 */
class InsertProtocol extends WriteProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.insert");

    private final InsertRequest insertRequest;

    InsertProtocol(final MongoNamespace namespace, final boolean ordered, final InsertRequest insertRequest) {
        super(namespace, ordered);
        this.insertRequest = insertRequest;
    }

    @Override
    public WriteConcernResult execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Inserting 1 document into namespace %s on connection [%s] to server %s",
                                getNamespace(),
                                connection.getDescription().getConnectionId(),
                                connection.getDescription().getServerAddress()));
        }
        WriteConcernResult writeConcernResult = super.execute(connection);
        LOGGER.debug("Insert completed");
        return writeConcernResult;
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<WriteConcernResult> callback) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously inserting 1 document into namespace %s on connection [%s] to server %s",
                                    getNamespace(), connection.getDescription().getConnectionId(),
                                    connection.getDescription().getServerAddress()));
            }
            super.executeAsync(connection, new SingleResultCallback<WriteConcernResult>() {
                @Override
                public void onResult(final WriteConcernResult result, final Throwable t) {
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
    protected BsonDocument getAsWriteCommand(final ByteBufferBsonOutput bsonOutput, final int firstDocumentPosition) {
        return getBaseCommandDocument("insert")
               .append("documents", new BsonArray(ByteBufBsonDocument.create(bsonOutput, firstDocumentPosition)));

    }

    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new InsertMessage(getNamespace().getFullName(), insertRequest, settings);
    }

    @Override
    protected com.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }
}
