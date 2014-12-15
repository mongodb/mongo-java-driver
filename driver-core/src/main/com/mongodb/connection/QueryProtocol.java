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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import static com.mongodb.connection.ProtocolHelper.encodeMessage;
import static com.mongodb.connection.ProtocolHelper.getMessageSettings;
import static com.mongodb.connection.ProtocolHelper.getQueryFailureException;
import static java.lang.String.format;

/**
 * An implementation of the MongoDB OP_QUERY wire protocol.
 *
 * @param <T> the type of document to decode query results to
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
class QueryProtocol<T> implements Protocol<QueryResult<T>> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.query");
    private final int skip;
    private final int numberToReturn;
    private final BsonDocument queryDocument;
    private final BsonDocument fields;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;
    private boolean tailableCursor;
    private boolean slaveOk;
    private boolean oplogReplay;
    private boolean noCursorTimeout;
    private boolean awaitData;
    private boolean partial;

    /**
     * Construct an instance.
     *
     * @param namespace      the namespace
     * @param skip           the number of documents to skip
     * @param numberToReturn the number to return
     * @param queryDocument  the query document
     * @param fields         the fields to return in the result documents
     * @param resultDecoder  the decoder for the result documents
     */
    public QueryProtocol(final MongoNamespace namespace, final int skip,
                         final int numberToReturn, final BsonDocument queryDocument,
                         final BsonDocument fields, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.skip = skip;
        this.numberToReturn = numberToReturn;
        this.queryDocument = queryDocument;
        this.fields = fields;
        this.resultDecoder = resultDecoder;
    }

    /**
     * Gets whether the cursor is configured to be a tailable cursor.
     *
     * <p>Tailable means the cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You
     * can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor",
     * the cursor may become invalid at some point - for example if the final object it references were deleted.</p>
     *
     * @return true if the cursor is configured to be a tailable cursor
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isTailableCursor() {
        return tailableCursor;
    }

    /**
     * Sets whether the cursor should be a tailable cursor.
     *
     * <p>Tailable means the cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You
     * can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor",
     * the cursor may become invalid at some point - for example if the final object it references were deleted.</p>
     *
     * @param tailableCursor whether the cursor should be a tailable cursor.
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public QueryProtocol<T> tailableCursor(final boolean tailableCursor) {
        this.tailableCursor = tailableCursor;
        return this;
    }

    /**
     * Returns true if set to allowed to query non-primary replica set members.
     *
     * @return true if set to allowed to query non-primary replica set members.
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isSlaveOk() {
        return slaveOk;
    }

    /**
     * Sets if allowed to query non-primary replica set members.
     *
     * @param slaveOk true if allowed to query non-primary replica set members.
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public QueryProtocol<T> slaveOk(final boolean slaveOk) {
        this.slaveOk = slaveOk;
        return this;
    }

    /**
     * Internal replication use only.  Driver users should ordinarily not use this.
     *
     * @return oplogReplay
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isOplogReplay() {
        return oplogReplay;
    }

    /**
     * Internal replication use only.  Driver users should ordinarily not use this.
     *
     * @param oplogReplay the oplogReplay value
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public QueryProtocol<T> oplogReplay(final boolean oplogReplay) {
        this.oplogReplay = oplogReplay;
        return this;
    }

    /**
     * Returns true if cursor timeout has been turned off.
     *
     * <p>The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use.</p>
     *
     * @return if cursor timeout has been turned off
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    /**
     * Sets if the cursor timeout should be turned off.
     *
     * @param noCursorTimeout true if the cursor timeout should be turned off.
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public QueryProtocol<T> noCursorTimeout(final boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    /**
     * Returns true if the cursor should await for data.
     *
     * <p>Use with {@link #tailableCursor}. If we are at the end of the data, block for a while rather than returning no data. After a
     * timeout period, we do return as normal.</p>
     *
     * @return if the cursor should await for data
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isAwaitData() {
        return awaitData;
    }

    /**
     * Sets if the cursor should await for data.
     *
     * <p>Use with {@link #tailableCursor}. If we are at the end of the data, block for a while rather than returning no data. After a
     * timeout period, we do return as normal.</p>
     *
     * @param awaitData if we should await for data
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public QueryProtocol<T> awaitData(final boolean awaitData) {
        this.awaitData = awaitData;
        return this;
    }

    /**
     * Returns true if can get partial results from a mongos if some shards are down.
     *
     * @return if can get partial results from a mongos if some shards are down
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isPartial() {
        return partial;
    }

    /**
     * Sets if partial results from a mongos if some shards are down are allowed
     *
     * @param partial allow partial results from a mongos if some shards are down
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public QueryProtocol<T> partial(final boolean partial) {
        this.partial = partial;
        return this;
    }

    @Override
    public QueryResult<T> execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Sending query of namespace %s on connection [%s] to server %s", namespace,
                                connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
        }
        QueryResult<T> queryResult = receiveMessage(connection, sendMessage(connection));
        LOGGER.debug("Query completed");
        return queryResult;
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<QueryResult<T>> callback) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously sending query of namespace %s on connection [%s] to server %s", namespace,
                                    connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
            }
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            QueryMessage message = createQueryMessage(connection.getDescription());
            encodeMessage(message, bsonOutput);
            SingleResultCallback<ResponseBuffers> receiveCallback = new QueryResultCallback<T>(namespace,
                                                                                               callback,
                                                                                               resultDecoder,
                                                                                               message.getId(),
                                                                                               connection.getDescription()
                                                                                                         .getServerAddress());
            connection.sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(),
                                        new SendMessageCallback<QueryResult<T>>(connection, bsonOutput, message.getId(), callback,
                                                                                receiveCallback));
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private QueryMessage createQueryMessage(final ConnectionDescription connectionDescription) {
        return (QueryMessage) new QueryMessage(namespace.getFullName(), skip, numberToReturn, queryDocument, fields,
                                               getMessageSettings(connectionDescription))
                                  .tailableCursor(isTailableCursor())
                                  .slaveOk(isSlaveOk())
                                  .oplogReplay(isOplogReplay())
                                  .noCursorTimeout(isNoCursorTimeout())
                                  .awaitData(isAwaitData())
                                  .partial(isPartial());
    }

    private QueryMessage sendMessage(final InternalConnection connection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            QueryMessage message = createQueryMessage(connection.getDescription());
            message.encode(bsonOutput);
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return message;
        } finally {
            bsonOutput.close();
        }
    }

    private QueryResult<T> receiveMessage(final InternalConnection connection, final QueryMessage message) {
        ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
        try {
            if (responseBuffers.getReplyHeader().isQueryFailure()) {
                BsonDocument errorDocument = new ReplyMessage<BsonDocument>(responseBuffers,
                                                                            new BsonDocumentCodec(),
                                                                            message.getId()).getDocuments().get(0);
                throw getQueryFailureException(errorDocument, connection.getDescription().getServerAddress());
            }
            ReplyMessage<T> replyMessage = new ReplyMessage<T>(responseBuffers, resultDecoder, message.getId());

            return new QueryResult<T>(namespace, replyMessage, connection.getDescription().getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }
}
