/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.protocol;

import org.bson.io.InputBuffer;

public class MongoReplyHeader {
    private static final int CURSOR_NOT_FOUND_RESPONSE_FLAG = 1;
    private static final int QUERY_FAILURE_RESPONSE_FLAG = 2;

    private final int messageLength;
    private final int requestId;
    private final int responseTo;
    private final int opCode;
    private final int responseFlags;
    private final long cursorId;
    private final int startingFrom;
    private final int numberReturned;

    public MongoReplyHeader(final InputBuffer headerInputBuffer) {
        messageLength = headerInputBuffer.readInt32();
        requestId = headerInputBuffer.readInt32();
        responseTo = headerInputBuffer.readInt32();  // TODO: validate that this is a response to the expected message
        opCode = headerInputBuffer.readInt32();  // TODO: check for validity
        responseFlags = headerInputBuffer.readInt32();
        cursorId = headerInputBuffer.readInt64();
        startingFrom = headerInputBuffer.readInt32();
        numberReturned = headerInputBuffer.readInt32();
    }

    public int getMessageLength() {
        return messageLength;
    }

    public int getRequestId() {
        return requestId;
    }

    public int getResponseTo() {
        return responseTo;
    }

    public int getOpCode() {
        return opCode;
    }

    public int getResponseFlags() {
        return responseFlags;
    }

    public long getCursorId() {
        return cursorId;
    }

    public int getStartingFrom() {
        return startingFrom;
    }

    public int getNumberReturned() {
        return numberReturned;
    }

    public boolean isCursorNotFound() {
        return (responseFlags & CURSOR_NOT_FOUND_RESPONSE_FLAG) == CURSOR_NOT_FOUND_RESPONSE_FLAG;
    }

    public boolean isQueryFailure() {
        return (responseFlags & QUERY_FAILURE_RESPONSE_FLAG) == QUERY_FAILURE_RESPONSE_FLAG;
    }
}
