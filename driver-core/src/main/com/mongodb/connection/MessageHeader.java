/*
 * Copyright 2017 MongoDB, Inc.
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

// Contains the details of an OP_COMPRESSED reply from a MongoDB server.
class MessageHeader {
    /**
     * The length of the standard message header in the MongoDB wire protocol.
     */
    public static final int MESSAGE_HEADER_LENGTH = 16;

    private final int messageLength;
    private final int requestId;
    private final int responseTo;
    private final int opCode;

    MessageHeader(final ByteBuf header, final int maxMessageLength) {
        messageLength = header.getInt();
        requestId = header.getInt();
        responseTo = header.getInt();
        opCode = header.getInt();

        if (messageLength > maxMessageLength) {
            throw new MongoInternalException(String.format("The reply message length %d is less than the maximum message length %d",
                    messageLength, maxMessageLength));
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

    /**
     * Gets the opcode
     *
     * @return the opcode
     */
    public int getOpCode() {
        return opCode;
    }
}
