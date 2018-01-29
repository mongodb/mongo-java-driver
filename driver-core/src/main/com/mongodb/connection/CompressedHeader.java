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

package com.mongodb.connection;

import com.mongodb.MongoInternalException;
import org.bson.ByteBuf;

import static com.mongodb.connection.MessageHeader.MESSAGE_HEADER_LENGTH;
import static com.mongodb.connection.OpCode.OP_COMPRESSED;
import static java.lang.String.format;

// Contains the details of an OP_COMPRESSED reply from a MongoDB server.
class CompressedHeader {
    /**
     * The length of the reply header in the MongoDB wire protocol.
     */
    public static final int COMPRESSED_HEADER_LENGTH = 9;

    /**
     * The length of the OP_COMPRESSED header plus the length of the standard message header
     */
    public static final int TOTAL_COMPRESSED_HEADER_LENGTH = COMPRESSED_HEADER_LENGTH + MESSAGE_HEADER_LENGTH;

    private final int originalOpcode;
    private final int uncompressedSize;
    private final byte compressorId;
    private final MessageHeader messageHeader;

    CompressedHeader(final ByteBuf header, final MessageHeader messageHeader) {
        this.messageHeader = messageHeader;

        if (messageHeader.getOpCode() != OP_COMPRESSED.getValue()) {
            throw new MongoInternalException(format("The reply message opCode %d does not match the expected opCode %d",
                    messageHeader.getOpCode(), OP_COMPRESSED.getValue()));
        }

        if (messageHeader.getMessageLength() < TOTAL_COMPRESSED_HEADER_LENGTH) {
            throw new MongoInternalException(format("The reply message length %d is less than the mimimum message length %d",
                    messageHeader.getMessageLength(), COMPRESSED_HEADER_LENGTH));
        }

        originalOpcode = header.getInt();
        uncompressedSize = header.getInt();
        compressorId = header.get();
    }

    /**
     *
     * @return the original opcode
     */
    public int getOriginalOpcode() {
        return originalOpcode;
    }

    /**
     *
     * @return the uncompressed size of the wrapped message
     */
    public int getUncompressedSize() {
        return uncompressedSize;
    }

    /**
     *
     * @return the compressor identifier with which the message is compressed
     */
    public byte getCompressorId() {
        return compressorId;
    }

    /**
     * @return the size of the compressed message.
     */
    public int getCompressedSize() {
        return messageHeader.getMessageLength() - COMPRESSED_HEADER_LENGTH - MESSAGE_HEADER_LENGTH;
    }

    public MessageHeader getMessageHeader() {
        return messageHeader;
    }
}
