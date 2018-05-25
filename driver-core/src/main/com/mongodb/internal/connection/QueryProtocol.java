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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ByteBufferBsonOutput;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.internal.connection.ProtocolHelper.encodeMessageWithMetadata;
import static com.mongodb.internal.connection.ProtocolHelper.getMessageSettings;
import static com.mongodb.internal.connection.ProtocolHelper.getQueryFailureException;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandSucceededEvent;
import static java.lang.String.format;

/**
 * An implementation of the MongoDB OP_QUERY wire protocol.
 *
 * @param <T> the type of document to decode query results to
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
class QueryProtocol<T> implements LegacyProtocol<QueryResult<T>> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.query");
    private static final String FIND_COMMAND_NAME = "find";
    private static final String EXPLAIN_COMMAND_NAME = "explain";
    private final int skip;
    private final int limit;
    private final int batchSize;
    private final int numberToReturn;
    private final boolean withLimitAndBatchSize;
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
    private CommandListener commandListener;

    QueryProtocol(final MongoNamespace namespace, final int skip, final int numberToReturn, final BsonDocument queryDocument,
                  final BsonDocument fields, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.skip = skip;
        this.withLimitAndBatchSize = false;
        this.numberToReturn = numberToReturn;
        this.limit = 0;
        this.batchSize = 0;
        this.queryDocument = queryDocument;
        this.fields = fields;
        this.resultDecoder = resultDecoder;
    }

    QueryProtocol(final MongoNamespace namespace, final int skip, final int limit, final int batchSize,
                  final BsonDocument queryDocument, final BsonDocument fields, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.skip = skip;
        this.withLimitAndBatchSize = true;
        this.numberToReturn = 0;
        this.limit = limit;
        this.batchSize = batchSize;
        this.queryDocument = queryDocument;
        this.fields = fields;
        this.resultDecoder = resultDecoder;
    }

    public void setCommandListener(final CommandListener commandListener) {
        this.commandListener = commandListener;
    }

    public CommandListener getCommandListener() {
        return commandListener;
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
        long startTimeNanos = System.nanoTime();
        QueryMessage message = null;
        try {
            boolean isExplain = false;
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            try {
                message = createQueryMessage(connection.getDescription());
                message.encode(bsonOutput, NoOpSessionContext.INSTANCE);
                isExplain = sendQueryStartedEvent(connection, message, bsonOutput, message.getEncodingMetadata());
                connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            } finally {
                bsonOutput.close();
            }

            ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
            try {
                if (responseBuffers.getReplyHeader().isQueryFailure()) {
                    BsonDocument errorDocument = new ReplyMessage<BsonDocument>(responseBuffers,
                                                                                new BsonDocumentCodec(),
                                                                                message.getId()).getDocuments().get(0);
                    throw getQueryFailureException(errorDocument, connection.getDescription().getServerAddress());
                }
                ReplyMessage<T> replyMessage = new ReplyMessage<T>(responseBuffers, resultDecoder, message.getId());
                QueryResult<T> result = new QueryResult<T>(namespace, replyMessage.getDocuments(),
                        replyMessage.getReplyHeader().getCursorId(), connection.getDescription().getServerAddress());

                sendQuerySucceededEvent(connection.getDescription(), startTimeNanos, message, isExplain, responseBuffers, result);

                LOGGER.debug("Query completed");

                return result;
            } finally {
                responseBuffers.close();
            }

        } catch (RuntimeException e) {
            if (commandListener != null) {
                sendCommandFailedEvent(message, FIND_COMMAND_NAME, connection.getDescription(), System.nanoTime() - startTimeNanos, e,
                        commandListener);
            }
            throw e;
        }
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<QueryResult<T>> callback) {
        long startTimeNanos = System.nanoTime();
        QueryMessage message = createQueryMessage(connection.getDescription());
        boolean sentStartedEvent = true;
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously sending query of namespace %s on connection [%s] to server %s", namespace,
                                    connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
            }
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            RequestMessage.EncodingMetadata metadata = encodeMessageWithMetadata(message, bsonOutput);
            boolean isExplainEvent = sendQueryStartedEvent(connection, message, bsonOutput, metadata);
            sentStartedEvent = true;

            SingleResultCallback<ResponseBuffers> receiveCallback = new QueryResultCallback(callback, message.getId(),
                    startTimeNanos, message, isExplainEvent, connection.getDescription());
            connection.sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(),
                                        new SendMessageCallback<QueryResult<T>>(connection, bsonOutput, message,
                                                getCommandName(isExplainEvent), startTimeNanos, commandListener, callback,
                                                receiveCallback));
        } catch (Throwable t) {
            if (commandListener != null) {
                sendCommandFailedEvent(message, FIND_COMMAND_NAME, connection.getDescription(), System.nanoTime() - startTimeNanos, t,
                        commandListener);
            }
            callback.onResult(null, t);
        }
    }

    private boolean sendQueryStartedEvent(final InternalConnection connection, final QueryMessage message,
                                          final ByteBufferBsonOutput bsonOutput, final RequestMessage.EncodingMetadata metadata) {
        boolean isExplainEvent = false;
        if (commandListener != null) {
            BsonDocument command = asFindCommandDocument(bsonOutput, metadata.getFirstDocumentPosition());
            isExplainEvent = command.keySet().iterator().next().equals(EXPLAIN_COMMAND_NAME);
            sendCommandStartedEvent(message, namespace.getDatabaseName(),
                    getCommandName(isExplainEvent),
                    command,
                    connection.getDescription(), commandListener);
        }
        return isExplainEvent;
    }

    private String getCommandName(final boolean isExplainEvent) {
        return isExplainEvent ? EXPLAIN_COMMAND_NAME : FIND_COMMAND_NAME;
    }

    private void sendQuerySucceededEvent(final ConnectionDescription connectionDescription, final long startTimeNanos,
                                         final QueryMessage message,
                                         final boolean isExplainEvent, final ResponseBuffers responseBuffers,
                                         final QueryResult<T> queryResult) {
        if (commandListener != null) {
            BsonDocument response = asFindCommandResponseDocument(responseBuffers, queryResult, isExplainEvent);
            sendCommandSucceededEvent(message, getCommandName(isExplainEvent), response, connectionDescription,
                    System.nanoTime() - startTimeNanos, commandListener);
        }
    }

    private QueryMessage createQueryMessage(final ConnectionDescription connectionDescription) {
        return (QueryMessage) new QueryMessage(namespace.getFullName(), skip, getNumberToReturn(), queryDocument, fields,
                                               getMessageSettings(connectionDescription))
                                  .tailableCursor(isTailableCursor())
                                  .slaveOk(isSlaveOk())
                                  .oplogReplay(isOplogReplay())
                                  .noCursorTimeout(isNoCursorTimeout())
                                  .awaitData(isAwaitData())
                                  .partial(isPartial());
    }

    private int getNumberToReturn() {
        if (withLimitAndBatchSize) {
            if (limit < 0) {
                return limit;
            } else if (limit == 0) {
                return batchSize;
            } else if (batchSize == 0) {
                return limit;
            } else if (limit < Math.abs(batchSize)) {
                return limit;
            } else {
                return batchSize;
            }
        } else {
            return numberToReturn;
        }
    }

    private static final Map<String, String> META_OPERATOR_TO_COMMAND_FIELD_MAP = new HashMap<String, String>();

    static {
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$query", "filter");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$orderby", "sort");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$hint", "hint");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$comment", "comment");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$maxScan", "maxScan");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$maxTimeMS", "maxTimeMS");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$max", "max");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$min", "min");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$returnKey", "returnKey");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$showDiskLoc", "showRecordId");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$snapshot", "snapshot");
    }

    private BsonDocument asFindCommandDocument(final ByteBufferBsonOutput bsonOutput, final int firstDocumentPosition) {
        BsonDocument command = new BsonDocument(FIND_COMMAND_NAME, new BsonString(namespace.getCollectionName()));

        boolean isExplain = false;

        List<ByteBufBsonDocument> documents = ByteBufBsonDocument.createList(bsonOutput, firstDocumentPosition);

        ByteBufBsonDocument rawQueryDocument = documents.get(0);
        for (Map.Entry<String, BsonValue> cur : rawQueryDocument.entrySet()) {
            String commandFieldName = META_OPERATOR_TO_COMMAND_FIELD_MAP.get(cur.getKey());
            if (commandFieldName != null) {
                command.append(commandFieldName, cur.getValue());
            } else if (cur.getKey().equals("$explain")) {
                isExplain = true;
            }
        }

        if (command.size() == 1) {
            command.append("filter", rawQueryDocument);
        }

        if (documents.size() == 2) {
            command.append("projection", documents.get(1));
        }

        if (skip != 0) {
            command.append("skip", new BsonInt32(skip));
        }

        if (withLimitAndBatchSize) {
            if (limit != 0) {
                command.append("limit", new BsonInt32(limit));
            }
            if (batchSize != 0) {
                command.append("batchSize", new BsonInt32(batchSize));
            }
        }

        if (tailableCursor) {
            command.append("tailable", BsonBoolean.valueOf(tailableCursor));
        }
        if (noCursorTimeout) {
            command.append("noCursorTimeout", BsonBoolean.valueOf(noCursorTimeout));
        }
        if (oplogReplay) {
            command.append("oplogReplay", BsonBoolean.valueOf(oplogReplay));
        }
        if (awaitData) {
            command.append("awaitData", BsonBoolean.valueOf(awaitData));
        }
        if (partial) {
            command.append("allowPartialResults", BsonBoolean.valueOf(partial));
        }

        if (isExplain) {
            command = new BsonDocument(EXPLAIN_COMMAND_NAME, command);
        }

        return command;
    }

    private BsonDocument asFindCommandResponseDocument(final ResponseBuffers responseBuffers, final QueryResult<T> queryResult,
                                                       final boolean isExplain) {
        List<ByteBufBsonDocument> rawResultDocuments = Collections.emptyList();
        if (responseBuffers.getReplyHeader().getNumberReturned() > 0) {
            responseBuffers.reset();
            rawResultDocuments = ByteBufBsonDocument.createList(responseBuffers);
        }

        if (isExplain) {
            BsonDocument explainCommandResponseDocument = new BsonDocument("ok", new BsonDouble(1));
            explainCommandResponseDocument.putAll(rawResultDocuments.get(0));
            return explainCommandResponseDocument;
        } else {
            BsonDocument cursorDocument = new BsonDocument("id",
                                                           queryResult.getCursor() == null
                                                           ? new BsonInt64(0) : new BsonInt64(queryResult.getCursor().getId()))
                                          .append("ns", new BsonString(namespace.getFullName()))
                                          .append("firstBatch", new BsonArray(rawResultDocuments));

            return new BsonDocument("cursor", cursorDocument)
                   .append("ok", new BsonDouble(1));
        }
    }

    class QueryResultCallback extends ResponseCallback {
        private final SingleResultCallback<QueryResult<T>> callback;
        private final ConnectionDescription connectionDescription;
        private final long startTimeNanos;
        private final QueryMessage message;
        private final boolean isExplainEvent;

        QueryResultCallback(final SingleResultCallback<QueryResult<T>> callback, final int requestId, final long startTimeNanos,
                            final QueryMessage message, final boolean isExplainEvent, final ConnectionDescription connectionDescription) {
            super(requestId, connectionDescription.getServerAddress());
            this.callback = callback;
            this.startTimeNanos = startTimeNanos;
            this.message = message;
            this.isExplainEvent = isExplainEvent;
            this.connectionDescription = connectionDescription;
        }

        @Override
        protected void callCallback(final ResponseBuffers responseBuffers, final Throwable throwableFromCallback) {
            try {
                if (throwableFromCallback != null) {
                    throw throwableFromCallback;
                } else if (responseBuffers.getReplyHeader().isQueryFailure()) {
                    BsonDocument errorDocument = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(),
                                                                                getRequestId()).getDocuments().get(0);
                    throw getQueryFailureException(errorDocument, getServerAddress());
                } else {
                    ReplyMessage<T> replyMessage = new ReplyMessage<T>(responseBuffers, resultDecoder, getRequestId());
                    QueryResult<T> result = new QueryResult<T>(namespace, replyMessage.getDocuments(),
                            replyMessage.getReplyHeader().getCursorId(), getServerAddress());

                    sendQuerySucceededEvent(connectionDescription, startTimeNanos, message, isExplainEvent, responseBuffers, result);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Query results received %s documents with cursor %s",
                                result.getResults().size(),
                                result.getCursor()));
                    }
                    callback.onResult(result, null);
                }
            } catch (Throwable t) {
                if (commandListener != null) {
                    sendCommandFailedEvent(message, FIND_COMMAND_NAME, connectionDescription, System.nanoTime() - startTimeNanos, t,
                            commandListener);
                }
                callback.onResult(null, t);
            } finally {
                try {
                    if (responseBuffers != null) {
                        responseBuffers.close();
                    }
                } catch (Throwable t1) {
                    LOGGER.debug("GetMore ResponseBuffer close exception", t1);
                }
            }
        }
    }
}
