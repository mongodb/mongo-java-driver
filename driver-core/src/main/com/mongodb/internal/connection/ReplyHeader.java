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

import static com.mongodb.internal.connection.MessageHeader.MESSAGE_HEADER_LENGTH;
import static com.mongodb.internal.connection.OpCode.OP_MSG;
import static com.mongodb.internal.connection.OpCode.OP_REPLY;
import static java.lang.String.format;

/**
 * Contains the details of a reply from a MongoDB server.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class ReplyHeader {
    /**
     * The length of the OP_REPLY header in the MongoDB wire protocol.
     */
    public static final int REPLY_HEADER_LENGTH = 20;

    /**
     * The length of the OP_REPLY header plus the length of the standard message header
     */
    public static final int TOTAL_REPLY_HEADER_LENGTH = REPLY_HEADER_LENGTH + MESSAGE_HEADER_LENGTH;

    private final int messageLength;
    private final int requestId;
    private final int responseTo;
    private final boolean hasMoreToCome;

    ReplyHeader(final ByteBuf header, final MessageHeader messageHeader) {
        this(messageHeader.getMessageLength(), messageHeader.getOpCode(), messageHeader, header);
    }

    ReplyHeader(final ByteBuf header, final CompressedHeader compressedHeader) {
        this(compressedHeader.getUncompressedSize() + MESSAGE_HEADER_LENGTH, compressedHeader.getOriginalOpcode(),
                compressedHeader.getMessageHeader(), header);
    }

    private ReplyHeader(final int messageLength, final int opCode, final MessageHeader messageHeader, final ByteBuf header) {
        this.messageLength = messageLength;
        this.requestId = messageHeader.getRequestId();
        this.responseTo = messageHeader.getResponseTo();
        if (opCode == OP_MSG.getValue()) {
            int flagBits = header.getInt();
            hasMoreToCome = (flagBits & (1 << 1)) != 0;
            header.get();  // ignored payload type
        } else if (opCode == OP_REPLY.getValue()) {
            if (messageLength < TOTAL_REPLY_HEADER_LENGTH) {
                throw new MongoInternalException(format("The reply message length %d is less than the minimum message length %d",
                        messageLength, TOTAL_REPLY_HEADER_LENGTH));
            }
            hasMoreToCome = false;

            header.getInt();  // ignored responseFlags
            header.getLong(); // ignored cursorId
            header.getInt();  // ignored startingFrom
            int numberReturned = header.getInt();

            if (numberReturned != 1) {
                throw new MongoInternalException(format("The reply message number of returned documents, %d, is expected to be 1",
                        numberReturned));
            }
        } else {
            throw new MongoInternalException(format("Unexpected reply message opCode %d", opCode));
        }
    }


    /**
     * Gets the total size of the message in bytes. This total includes the 4 bytes that holds the message length.
     *
     * @return the total message size, including all of the header
     */
    public int getMessageLength() {
        return messageLength;
    }

    /**
     * This is a client or database-generated identifier that uniquely identifies this message. Along with the {@code responseTo} field in
     * responses, clients can use this to associate query responses with the originating query.
     *
     * @return the identifier for this message
     */
    public int getRequestId() {
        return requestId;
    }

    /**
     * Along with the requestID field in queries, clients can use this to associate query responses with the originating query.
     *
     * @return the request ID from the original request
     */
    public int getResponseTo() {
        return responseTo;
    }

    public boolean hasMoreToCome() {
        return hasMoreToCome;
    }
}
