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
import org.bson.ByteBuf;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("ReplyHeader")
class ReplyHeaderTest {

    @ParameterizedTest(name = "with responseFlags {0}")
    @ValueSource(ints = {0, 1, 2, 3})
    @DisplayName("should parse reply header with various response flags")
    void testParseReplyHeader(final int responseFlags) {
        try (BasicOutputBuffer outputBuffer = new BasicOutputBuffer()) {
            outputBuffer.writeInt(186);
            outputBuffer.writeInt(45);
            outputBuffer.writeInt(23);
            outputBuffer.writeInt(1);
            outputBuffer.writeInt(responseFlags);
            outputBuffer.writeLong(9000);
            outputBuffer.writeInt(4);
            outputBuffer.writeInt(1);

            ByteBuf byteBuf = outputBuffer.getByteBuffers().get(0);
            ReplyHeader replyHeader = new ReplyHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize()));

            assertEquals(186, replyHeader.getMessageLength());
            assertEquals(45, replyHeader.getRequestId());
            assertEquals(23, replyHeader.getResponseTo());
        }
    }

    @ParameterizedTest(name = "with responseFlags {0}")
    @ValueSource(ints = {0, 1, 2, 3})
    @DisplayName("should parse reply header with compressed header and various response flags")
    void testParseReplyHeaderWithCompressedHeader(final int responseFlags) {
        try (BasicOutputBuffer outputBuffer = new BasicOutputBuffer()) {
            outputBuffer.writeInt(186);
            outputBuffer.writeInt(45);
            outputBuffer.writeInt(23);
            outputBuffer.writeInt(2012);
            outputBuffer.writeInt(1);
            outputBuffer.writeInt(258);
            outputBuffer.writeByte(2);
            outputBuffer.writeInt(responseFlags);
            outputBuffer.writeLong(9000);
            outputBuffer.writeInt(4);
            outputBuffer.writeInt(1);

            ByteBuf byteBuf = outputBuffer.getByteBuffers().get(0);
            CompressedHeader compressedHeader = new CompressedHeader(byteBuf,
                    new MessageHeader(byteBuf, getDefaultMaxMessageSize()));
            ReplyHeader replyHeader = new ReplyHeader(byteBuf, compressedHeader);

            assertEquals(274, replyHeader.getMessageLength());
            assertEquals(45, replyHeader.getRequestId());
            assertEquals(23, replyHeader.getResponseTo());
        }
    }

    @Test
    @DisplayName("should throw MongoInternalException on incorrect opCode")
    void testThrowExceptionOnIncorrectOpCode() {
        try (BasicOutputBuffer outputBuffer = new BasicOutputBuffer()) {
            outputBuffer.writeInt(36);
            outputBuffer.writeInt(45);
            outputBuffer.writeInt(23);
            outputBuffer.writeInt(2);
            outputBuffer.writeInt(0);
            outputBuffer.writeLong(2);
            outputBuffer.writeInt(0);
            outputBuffer.writeInt(0);

            ByteBuf byteBuf = outputBuffer.getByteBuffers().get(0);

            MongoInternalException ex = assertThrows(MongoInternalException.class,
                    () -> new ReplyHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize())));

            assertEquals("Unexpected reply message opCode 2", ex.getMessage());
        }
    }

    @Test
    @DisplayName("should throw MongoInternalException on message size less than 36 bytes")
    void testThrowExceptionOnMessageSizeLessThan36() {
        try (BasicOutputBuffer outputBuffer = new BasicOutputBuffer()) {
            outputBuffer.writeInt(35);
            outputBuffer.writeInt(45);
            outputBuffer.writeInt(23);
            outputBuffer.writeInt(1);
            outputBuffer.writeInt(0);
            outputBuffer.writeLong(2);
            outputBuffer.writeInt(0);
            outputBuffer.writeInt(0);

            ByteBuf byteBuf = outputBuffer.getByteBuffers().get(0);

            MongoInternalException ex = assertThrows(MongoInternalException.class,
                    () -> new ReplyHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize())));

            assertEquals("The reply message length 35 is less than the minimum message length 36", ex.getMessage());
        }
    }

    @Test
    @DisplayName("should throw MongoInternalException on message size exceeding max message size")
    void testThrowExceptionOnMessageSizeExceedingMax() {
        try (BasicOutputBuffer outputBuffer = new BasicOutputBuffer()) {
            outputBuffer.writeInt(400);
            outputBuffer.writeInt(45);
            outputBuffer.writeInt(23);
            outputBuffer.writeInt(1);
            outputBuffer.writeInt(0);
            outputBuffer.writeLong(2);
            outputBuffer.writeInt(0);
            outputBuffer.writeInt(0);

            ByteBuf byteBuf = outputBuffer.getByteBuffers().get(0);

            MongoInternalException ex = assertThrows(MongoInternalException.class,
                    () -> new ReplyHeader(byteBuf, new MessageHeader(byteBuf, 399)));

            assertEquals("The reply message length 400 is greater than the maximum message length 399", ex.getMessage());
        }
    }

    @Test
    @DisplayName("should throw MongoInternalException on negative number of returned documents")
    void testThrowExceptionOnNegativeNumberOfDocuments() {
        try (BasicOutputBuffer outputBuffer = new BasicOutputBuffer()) {
            outputBuffer.writeInt(186);
            outputBuffer.writeInt(45);
            outputBuffer.writeInt(23);
            outputBuffer.writeInt(1);
            outputBuffer.writeInt(1);
            outputBuffer.writeLong(9000);
            outputBuffer.writeInt(4);
            outputBuffer.writeInt(-1);

            ByteBuf byteBuf = outputBuffer.getByteBuffers().get(0);

            MongoInternalException ex = assertThrows(MongoInternalException.class,
                    () -> new ReplyHeader(byteBuf, new MessageHeader(byteBuf, getDefaultMaxMessageSize())));

            assertEquals("The reply message number of returned documents, -1, is expected to be 1", ex.getMessage());
        }
    }

    @Test
    @DisplayName("should throw MongoInternalException on negative number of documents with compressed header")
    void testThrowExceptionOnNegativeNumberOfDocumentsWithCompressedHeader() {
        try (BasicOutputBuffer outputBuffer = new BasicOutputBuffer()) {
            outputBuffer.writeInt(186);
            outputBuffer.writeInt(45);
            outputBuffer.writeInt(23);
            outputBuffer.writeInt(2012);
            outputBuffer.writeInt(1);
            outputBuffer.writeInt(258);
            outputBuffer.writeByte(2);
            outputBuffer.writeInt(1);
            outputBuffer.writeLong(9000);
            outputBuffer.writeInt(4);
            outputBuffer.writeInt(-1);

            ByteBuf byteBuf = outputBuffer.getByteBuffers().get(0);
            CompressedHeader compressedHeader = new CompressedHeader(byteBuf,
                    new MessageHeader(byteBuf, getDefaultMaxMessageSize()));

            MongoInternalException ex = assertThrows(MongoInternalException.class,
                    () -> new ReplyHeader(byteBuf, compressedHeader));

            assertEquals("The reply message number of returned documents, -1, is expected to be 1", ex.getMessage());
        }
    }
}
