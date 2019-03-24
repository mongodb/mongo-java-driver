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

import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.event.CommandListener;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.io.OutputBuffer;

import static com.mongodb.internal.connection.ProtocolHelper.encodeMessageWithMetadata;
import static com.mongodb.internal.connection.ProtocolHelper.getMessageSettings;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandSucceededEvent;

/**
 * Base class for legacy wire protocol messages that perform unacknowledged writes.
 */
abstract class WriteProtocol implements LegacyProtocol<WriteConcernResult> {

    private final MongoNamespace namespace;
    private final boolean ordered;
    private CommandListener commandListener;

    WriteProtocol(final MongoNamespace namespace, final boolean ordered) {
        this.namespace = namespace;
        this.ordered = ordered;
    }

    @Override
    public void setCommandListener(final CommandListener commandListener) {
        this.commandListener = commandListener;
    }

    @Override
    public WriteConcernResult execute(final InternalConnection connection) {
        RequestMessage requestMessage = null;
        long startTimeNanos = System.nanoTime();
        int messageId;
        boolean sentCommandStartedEvent = false;
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            requestMessage = createRequestMessage(getMessageSettings(connection.getDescription()));
            requestMessage.encode(bsonOutput, NoOpSessionContext.INSTANCE);
            sendStartedEvent(connection, requestMessage, requestMessage.getEncodingMetadata(), bsonOutput);
            sentCommandStartedEvent = true;

            messageId = requestMessage.getId();
            connection.sendMessage(bsonOutput.getByteBuffers(), messageId);
        } catch (RuntimeException e) {
            sendFailedEvent(connection, requestMessage, sentCommandStartedEvent, e, startTimeNanos);
            throw e;
        } finally {
            bsonOutput.close();
        }

        sendSucceededEvent(connection, requestMessage, startTimeNanos);

        return WriteConcernResult.unacknowledged();
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<WriteConcernResult> callback) {
        executeAsync(createRequestMessage(getMessageSettings(connection.getDescription())), connection, callback);
    }

    private void executeAsync(final RequestMessage requestMessage, final InternalConnection connection,
                              final SingleResultCallback<WriteConcernResult> callback) {
        long startTimeNanos = System.nanoTime();
        boolean sentCommandStartedEvent = false;
        try {
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);

            RequestMessage.EncodingMetadata encodingMetadata = encodeMessageWithMetadata(requestMessage, bsonOutput);
            sendStartedEvent(connection, requestMessage, encodingMetadata, bsonOutput);
            sentCommandStartedEvent = true;

            connection.sendMessageAsync(bsonOutput.getByteBuffers(), requestMessage.getId(),
                    new UnacknowledgedWriteResultCallback(callback, requestMessage, bsonOutput, connection, startTimeNanos));
        } catch (Throwable t) {
            sendFailedEvent(connection, requestMessage, sentCommandStartedEvent, t, startTimeNanos);
            callback.onResult(null, t);
        }
    }

    protected abstract BsonDocument getAsWriteCommand(ByteBufferBsonOutput bsonOutput, int firstDocumentPosition);

    protected BsonDocument getBaseCommandDocument(final String commandName) {
        BsonDocument baseCommandDocument = new BsonDocument(commandName, new BsonString(getNamespace().getCollectionName()))
                .append("ordered", BsonBoolean.valueOf(isOrdered()));
        baseCommandDocument.append("writeConcern", WriteConcern.UNACKNOWLEDGED.asDocument());
        return baseCommandDocument;
    }

    protected String getCommandName(final RequestMessage message) {
        switch (message.getOpCode()) {
            case OP_INSERT:
                return "insert";
            case OP_UPDATE:
                return "update";
            case OP_DELETE:
                return "delete";
            default:
                throw new MongoInternalException("Unexpected op code for write: " + message.getOpCode());
        }
    }


    private void sendStartedEvent(final InternalConnection connection, final RequestMessage message,
                                  final RequestMessage.EncodingMetadata encodingMetadata, final ByteBufferBsonOutput bsonOutput) {
        if (commandListener != null) {
            sendCommandStartedEvent(message, namespace.getDatabaseName(), getCommandName(message),
                    getAsWriteCommand(bsonOutput, encodingMetadata.getFirstDocumentPosition()),
                    connection.getDescription(), commandListener);
        }
    }

    private void sendSucceededEvent(final InternalConnection connection, final RequestMessage message,
                                    final long startTimeNanos) {
        if (commandListener != null) {
            sendSucceededEvent(connection, message, getResponseDocument(), startTimeNanos);
        }
    }

    private void sendSucceededEvent(final InternalConnection connection, final RequestMessage message,
                                    final BsonDocument responseDocument, final long startTimeNanos) {
        if (commandListener != null) {
            sendCommandSucceededEvent(message, getCommandName(message), responseDocument, connection.getDescription(),
                    System.nanoTime() - startTimeNanos, commandListener);
        }
    }

    private void sendFailedEvent(final InternalConnection connection, final RequestMessage message,
                                 final boolean sentCommandStartedEvent, final Throwable t, final long startTimeNanos) {
        if (commandListener != null && sentCommandStartedEvent) {
            sendCommandFailedEvent(message, getCommandName(message), connection.getDescription(), System.nanoTime() - startTimeNanos, t,
                    commandListener);
        }
    }

    private BsonDocument getResponseDocument() {
        return new BsonDocument("ok", new BsonInt32(1));
    }

    /**
     * Create the initial request message for the write.
     *
     * @param settings the message settings
     * @return the request message
     */
    protected abstract RequestMessage createRequestMessage(MessageSettings settings);

    /**
     * Gets the namespace.
     *
     * @return the namespace
     */
    protected MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets whether the writes are ordered.
     *
     * @return true if ordered
     */
    protected boolean isOrdered() {
        return ordered;
    }

    /**
     * Gets the logger.
     *
     * @return the logger
     */
    protected abstract Logger getLogger();

    private final class UnacknowledgedWriteResultCallback implements SingleResultCallback<Void> {
        private final SingleResultCallback<WriteConcernResult> callback;
        private final RequestMessage message;
        private final OutputBuffer writtenBuffer;
        private final InternalConnection connection;
        private final long startTimeNanos;

        UnacknowledgedWriteResultCallback(final SingleResultCallback<WriteConcernResult> callback,
                                          final RequestMessage message,
                                          final OutputBuffer writtenBuffer,
                                          final InternalConnection connection, final long startTimeNanos) {
            this.callback = callback;
            this.message = message;
            this.connection = connection;
            this.writtenBuffer = writtenBuffer;
            this.startTimeNanos = startTimeNanos;
        }

        @Override
        public void onResult(final Void result, final Throwable t) {
            writtenBuffer.close();
            if (t != null) {
                sendFailedEvent(connection, message, true, t, startTimeNanos);
                callback.onResult(null, t);
            } else {
                sendSucceededEvent(connection, message, startTimeNanos);
                callback.onResult(WriteConcernResult.unacknowledged(), null);
            }
        }
    }
}
