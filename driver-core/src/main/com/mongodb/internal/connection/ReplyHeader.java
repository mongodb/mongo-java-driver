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

    private static final int CURSOR_NOT_FOUND_RESPONSE_FLAG = 1;
    private static final int QUERY_FAILURE_RESPONSE_FLAG = 2;

    private final int messageLength;
    private final int requestId;
    private final int responseTo;
    private final int responseFlags;
    private final long cursorId;
    private final int startingFrom;
    private final int numberReturned;
    private final int opMsgFlagBits;

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
            responseFlags = 0;
            cursorId = 0;
            startingFrom = 0;
            numberReturned = 1;

            opMsgFlagBits = header.getInt();
            header.get();  // ignore payload type
        } else if (opCode == OP_REPLY.getValue()) {
            if (messageLength < TOTAL_REPLY_HEADER_LENGTH) {
                throw new MongoInternalException(format("The reply message length %d is less than the mimimum message length %d",
                        messageLength, TOTAL_REPLY_HEADER_LENGTH));
            }

            responseFlags = header.getInt();
            cursorId = header.getLong();
            startingFrom = header.getInt();
            numberReturned = header.getInt();
            opMsgFlagBits = 0;

            if (numberReturned < 0) {
                throw new MongoInternalException(format("The reply message number of returned documents, %d, is less than 0",
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

    /**
     * Gets additional information about the response.
     * <ul>
     *     <li>0 - <i>CursorNotFound</i>: Set when getMore is called but the cursor id is not valid at the server. Returned with zero
     *     results.</li>
     *     <li>1 - <i>QueryFailure</i>: Set when query failed. Results consist of one document containing an "$err" field describing the
     *     failure.
     *     <li>2 - <i>ShardConfigStale</i>: Drivers should ignore this. Only mongos will ever see this set, in which case,
     *     it needs to update config from the server.
     *     <li>3 - <i>AwaitCapable</i>: Set when the server supports the AwaitData Query option. If it doesn't,
     *     a client should sleep a little between getMore's of a Tailable cursor. Mongod version 1.6 supports AwaitData and thus always
     *     sets AwaitCapable.
     *     <li>4-31 - <i>Reserved</i>: Ignore
     * </ul>
     *
     * @return bit vector - see details above
     */
    public int getResponseFlags() {
        return responseFlags;
    }

    /**
     * Gets the cursor ID that this response is a part of. If there are no more documents to fetch from the server, the cursor ID will be 0.
     * This cursor ID must be used in any messages used to get more data, and also must be closed by the client when no longer needed.
     *
     * @return cursor ID to use if the client needs to fetch more from the server
     */
    public long getCursorId() {
        return cursorId;
    }

    /**
     * Returns the position in the cursor that is the start point of this reply.
     *
     * @return where in the cursor this reply is starting
     */
    public int getStartingFrom() {
        return startingFrom;
    }

    /**
     * Gets the number of documents to expect in the body of this reply.
     *
     * @return number of documents in the reply
     */
    public int getNumberReturned() {
        return numberReturned;
    }

    /**
     * Gets whether this query was performed with a cursor ID that was not valid on the server.
     *
     * @return true if this reply indicates the request to get more data was performed with a cursor ID that's not valid on the server
     */
    public boolean isCursorNotFound() {
        return (responseFlags & CURSOR_NOT_FOUND_RESPONSE_FLAG) == CURSOR_NOT_FOUND_RESPONSE_FLAG;
    }

    /**
     * Gets whether the query failed or not.
     *
     * @return true if this reply indicates the query failed.
     */
    public boolean isQueryFailure() {
        return (responseFlags & QUERY_FAILURE_RESPONSE_FLAG) == QUERY_FAILURE_RESPONSE_FLAG;
    }

    public int getOpMsgFlagBits() {
        return opMsgFlagBits;
    }

    public boolean hasMoreToCome() {
        return (opMsgFlagBits & (1 << 1)) != 0;
    }
}
