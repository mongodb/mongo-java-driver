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

import com.mongodb.MongoException;
import com.mongodb.RequestContext;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.io.BsonInput;
import org.bson.io.ByteBufferBsonInput;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import static com.mongodb.internal.connection.ProtocolHelper.getCommandFailureException;
import static com.mongodb.internal.connection.ProtocolHelper.isCommandOk;

class TestInternalConnection implements InternalConnection {

    private static class Interaction {
        private ResponseBuffers responseBuffers;
        private RuntimeException receiveException;
        private RuntimeException sendException;
    }

    private final ConnectionDescription description;
    private final BufferProvider bufferProvider;
    private final Deque<Interaction> replies;
    private final List<BsonInput> sent;
    private boolean opened;
    private boolean closed;

    TestInternalConnection(final ServerId serverId) {
        this.description = new ConnectionDescription(new ConnectionId(serverId), 0, ServerType.UNKNOWN, 0, 0, 0,
                Collections.<String>emptyList());
        this.bufferProvider = new SimpleBufferProvider();

        this.replies = new LinkedList<Interaction>();
        this.sent = new LinkedList<BsonInput>();
    }

    public void enqueueReply(final ResponseBuffers responseBuffers) {
        Interaction interaction = new Interaction();
        interaction.responseBuffers = responseBuffers;
        replies.add(interaction);
    }

    public void enqueueSendMessageException(final RuntimeException e) {
        Interaction interaction = new Interaction();
        interaction.sendException = e;
        replies.add(interaction);
    }

    public void enqueueReceiveMessageException(final RuntimeException e) {
        Interaction interaction = new Interaction();
        interaction.receiveException = e;
        replies.add(interaction);
    }

    public List<BsonInput> getSent() {
        return sent;
    }

    @Override
    public ConnectionDescription getDescription() {
        return description;
    }

    @Override
    public ServerDescription getInitialServerDescription() {
        throw new UnsupportedOperationException();
    }

    public void open() {
        opened = true;
    }

    @Override
    public void openAsync(final SingleResultCallback<Void> callback) {
        opened = true;
        callback.onResult(null, null);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean opened() {
        return opened;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public int getGeneration() {
        return 0;
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        // repackage all byte buffers into a single byte buffer...
        int totalSize = 0;
        for (ByteBuf buf : byteBuffers) {
            totalSize += buf.remaining();
        }

        ByteBuffer combined = ByteBuffer.allocate(totalSize);
        for (ByteBuf buf : byteBuffers) {
            combined.put(buf.array(), 0, buf.remaining());
        }

        ((Buffer) combined).flip();

        Interaction interaction = replies.getFirst();
        if (interaction.responseBuffers != null) {
            ReplyHeader header = replaceResponseTo(interaction.responseBuffers.getReplyHeader(), lastRequestId);
            interaction.responseBuffers = (new ResponseBuffers(header, interaction.responseBuffers.getBodyByteBuffer()));

            sent.add(new ByteBufferBsonInput(new ByteBufNIO(combined)));
        } else if (interaction.sendException != null) {
            replies.removeFirst();
            throw interaction.sendException;
        }
    }

    @Override
    public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this);
        try {
            message.encode(bsonOutput, sessionContext);
            sendMessage(bsonOutput.getByteBuffers(), message.getId());
        } finally {
            bsonOutput.close();
        }
        ResponseBuffers responseBuffers = receiveMessage(message.getId());
        try {
            boolean commandOk = isCommandOk(new BsonBinaryReader(new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer())));
            responseBuffers.reset();
            if (!commandOk) {
                throw getCommandFailureException(getResponseDocument(responseBuffers, message, new BsonDocumentCodec()),
                        description.getServerAddress());
            }
            return new ReplyMessage<T>(responseBuffers, decoder, message.getId()).getDocuments().get(0);
        } finally {
            responseBuffers.close();
        }
    }

    @Override
    public <T> void send(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasMoreToCome() {
        throw new UnsupportedOperationException();
    }

    private <T extends BsonDocument> T getResponseDocument(final ResponseBuffers responseBuffers,
                                                           final CommandMessage commandMessage, final Decoder<T> decoder) {
        ReplyMessage<T> replyMessage = new ReplyMessage<T>(responseBuffers, decoder, commandMessage.getId());
        responseBuffers.reset();
        return replyMessage.getDocuments().get(0);
    }

    @Override
    public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder,
            final SessionContext sessionContext, final RequestContext requestContext, final SingleResultCallback<T> callback) {
        try {
            T result = sendAndReceive(message, decoder, sessionContext);
            callback.onResult(result, null);
        } catch (MongoException ex) {
            callback.onResult(null, ex);
        }
    }

    private ReplyHeader replaceResponseTo(final ReplyHeader header, final int responseTo) {
        ByteBuffer headerByteBuffer = ByteBuffer.allocate(36);
        headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerByteBuffer.putInt(header.getMessageLength());
        headerByteBuffer.putInt(header.getRequestId());
        headerByteBuffer.putInt(responseTo);
        headerByteBuffer.putInt(1);
        headerByteBuffer.putInt(header.getResponseFlags());
        headerByteBuffer.putLong(header.getCursorId());
        headerByteBuffer.putInt(header.getStartingFrom());
        headerByteBuffer.putInt(header.getNumberReturned());
        ((Buffer) headerByteBuffer).flip();

        ByteBufNIO buffer = new ByteBufNIO(headerByteBuffer);
        MessageHeader messageHeader = new MessageHeader(buffer, ConnectionDescription.getDefaultMaxMessageSize());
        return new ReplyHeader(buffer, messageHeader);    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        if (this.replies.isEmpty()) {
            throw new MongoException("Test was not setup properly as too many calls to receiveMessage occured.");
        }

        Interaction interaction = replies.removeFirst();
        if (interaction.responseBuffers != null) {
            return interaction.responseBuffers;
        } else {
            throw interaction.receiveException;
        }
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        try {
            sendMessage(byteBuffers, lastRequestId);
            callback.onResult(null, null);
        } catch (RuntimeException e) {
            callback.onResult(null, e);
        }
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        try {
            ResponseBuffers buffers = receiveMessage(responseTo);
            callback.onResult(buffers, null);
        } catch (MongoException ex) {
            callback.onResult(null, ex);
        }
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return this.bufferProvider.getBuffer(size);
    }
}
