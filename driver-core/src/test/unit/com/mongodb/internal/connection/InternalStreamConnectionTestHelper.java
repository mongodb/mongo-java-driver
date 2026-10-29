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

import com.mongodb.MongoCompressor;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize;
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxWriteBatchSize;
import static com.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize;
import static com.mongodb.internal.connection.MessageHeader.MESSAGE_HEADER_LENGTH;
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION;

/**
 * Shared fixtures for the {@link InternalStreamConnection} unit tests: an open connection over a {@link TestStream}
 * with a stubbed handshake, a ping command, and builders for OP_REPLY response headers and bodies.
 */
final class InternalStreamConnectionTestHelper {

    static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress());

    private InternalStreamConnectionTestHelper() {
    }

    static InternalStreamConnection createOpenConnection(final TestStream stream) {
        return createOpenConnection(stream, null);
    }

    static InternalStreamConnection createOpenConnection(final TestStream stream, @Nullable final CommandListener commandListener) {
        return createOpenConnection(stream, commandListener, Collections.emptyList());
    }

    static InternalStreamConnection createOpenConnection(final TestStream stream, @Nullable final CommandListener commandListener,
            final List<MongoCompressor> compressorList) {
        StreamFactory streamFactory = serverAddress -> stream;
        InternalConnectionInitializationDescription initDesc = createInitializationDescription();

        InternalConnectionInitializer initializer = new InternalConnectionInitializer() {
            @Override
            public InternalConnectionInitializationDescription startHandshake(final InternalConnection connection,
                    final OperationContext operationContext) {
                return initDesc;
            }

            @Override
            public InternalConnectionInitializationDescription finishHandshake(final InternalConnection connection,
                    final InternalConnectionInitializationDescription description, final OperationContext operationContext) {
                return initDesc;
            }

            @Override
            public void startHandshakeAsync(final InternalConnection connection, final OperationContext operationContext,
                    final SingleResultCallback<InternalConnectionInitializationDescription> callback) {
                callback.onResult(initDesc, null);
            }

            @Override
            public void finishHandshakeAsync(final InternalConnection connection,
                    final InternalConnectionInitializationDescription description, final OperationContext operationContext,
                    final SingleResultCallback<InternalConnectionInitializationDescription> callback) {
                callback.onResult(initDesc, null);
            }
        };

        InternalStreamConnection connection = new InternalStreamConnection(
                SINGLE, SERVER_ID, new TestConnectionGenerationSupplier(),
                streamFactory, compressorList, commandListener, initializer);
        connection.open(createOperationContext());
        return connection;
    }

    static InternalConnectionInitializationDescription createInitializationDescription() {
        ConnectionDescription connectionDescription = new ConnectionDescription(
                new ConnectionId(SERVER_ID, 1, 1L), LATEST_WIRE_VERSION,
                ServerType.STANDALONE, getDefaultMaxWriteBatchSize(),
                getDefaultMaxDocumentSize(), getDefaultMaxMessageSize(), Collections.emptyList());
        ServerDescription serverDescription = ServerDescription.builder()
                .ok(true)
                .state(ServerConnectionState.CONNECTED)
                .type(ServerType.STANDALONE)
                .address(new ServerAddress())
                .build();
        return new InternalConnectionInitializationDescription(connectionDescription, serverDescription);
    }

    static CommandMessage createPingCommand() {
        return new CommandMessage(
                "admin",
                new BsonDocument("ping", new BsonInt32(1)),
                NoOpFieldNameValidator.INSTANCE,
                primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                SINGLE,
                null);
    }

    static OperationContext createOperationContext() {
        return OperationContext.simpleOperationContext(TimeoutSettings.DEFAULT);
    }

    /**
     * Creates a valid OP_REPLY response body: reply header (20 bytes) + BSON {ok: 1}.
     */
    static ByteBuf createResponseBody() {
        return createResponseBody(new BsonDocument("ok", new BsonInt32(1)));
    }

    /**
     * Creates a valid OP_REPLY response body: reply header (20 bytes) + the given BSON document.
     */
    static ByteBuf createResponseBody(final BsonDocument document) {
        return new ByteBufNIO(createResponseBodyBuffer(document));
    }

    /**
     * Creates the raw buffer for a valid OP_REPLY response body: reply header (20 bytes) + {ok: 1}.
     */
    static ByteBuffer createResponseBodyBuffer() {
        return createResponseBodyBuffer(new BsonDocument("ok", new BsonInt32(1)));
    }

    /**
     * Creates the raw buffer for a valid OP_REPLY response body: reply header (20 bytes) + the given BSON document.
     */
    static ByteBuffer createResponseBodyBuffer(final BsonDocument document) {
        byte[] bson = toBson(document);
        ByteBuffer buffer = ByteBuffer.allocate(20 + bson.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0);     // responseFlags
        buffer.putLong(0);    // cursorId
        buffer.putInt(0);     // startingFrom
        buffer.putInt(1);     // numberReturned
        buffer.put(bson);
        ((Buffer) buffer).flip();
        return buffer;
    }

    /**
     * Creates a valid 16-byte wire protocol header (OP_REPLY) for the given responseTo.
     */
    static ByteBuf createValidResponseHeader(final int responseTo) {
        return createValidResponseHeader(responseTo, new BsonDocument("ok", new BsonInt32(1)));
    }

    /**
     * Creates a valid 16-byte wire protocol header (OP_REPLY) for the given responseTo,
     * sized for a body containing the given BSON document.
     */
    static ByteBuf createValidResponseHeader(final int responseTo, final BsonDocument document) {
        return createValidResponseHeaderForBodyLength(responseTo, 20 + toBson(document).length);
    }

    /**
     * Creates a valid 16-byte wire protocol header (OP_REPLY) for the given responseTo and body length.
     */
    static ByteBuf createValidResponseHeaderForBodyLength(final int responseTo, final int bodyLength) {
        ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_HEADER_LENGTH);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(MESSAGE_HEADER_LENGTH + bodyLength); // messageLength
        buffer.putInt(1);            // requestId (server's response message ID)
        buffer.putInt(responseTo);   // responseTo (echoes client's requestId)
        buffer.putInt(1);            // opCode = OP_REPLY
        ((Buffer) buffer).flip();

        return new ByteBufNIO(buffer);
    }

    static byte[] toBson(final BsonDocument document) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(
                new BsonBinaryWriter(outputBuffer), document,
                EncoderContext.builder().build());
        return outputBuffer.toByteArray();
    }
}
