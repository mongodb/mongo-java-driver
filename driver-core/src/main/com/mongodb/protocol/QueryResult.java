/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.protocol;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.protocol.message.ReplyMessage;

import java.util.List;

/**
 * A batch of query results.
 *
 * @param <T> the type of document to decode query results to
 * @since 3.0
 */
public class QueryResult<T> {
    private final List<T> results;
    private final long cursorId;
    private final ServerAddress serverAddress;
    private final int requestId;

    /**
     * Construct an instance.
     *
     * @param results       the query results
     * @param cursorId      the cursor id
     * @param serverAddress the server address
     * @param requestId     the request id of the response message
     */
    public QueryResult(final List<T> results, final long cursorId, final ServerAddress serverAddress, final int requestId) {
        this.results = results;
        this.cursorId = cursorId;
        this.serverAddress = serverAddress;
        this.requestId = requestId;
    }

    /**
     * Construct an instance.
     *
     * @param replyMessage the reply message
     * @param address      the server address
     */
    QueryResult(final ReplyMessage<T> replyMessage, final ServerAddress address) {
        this(replyMessage.getDocuments(), replyMessage.getReplyHeader().getCursorId(), address,
             replyMessage.getReplyHeader().getRequestId());
    }

    /**
     * Gets the cursor.
     *
     * @return the cursor, which may be null if it's been exhausted
     */
    public ServerCursor getCursor() {
        return cursorId == 0 ? null : new ServerCursor(cursorId, serverAddress);
    }

    /**
     * Gets the results.
     *
     * @return the results
     */
    public List<T> getResults() {
        return results;
    }

    /**
     * Gets the server address.
     *
     * @return the server address
     */
    public ServerAddress getAddress() {
        return serverAddress;
    }

    /**
     * Gets the request id of the response message.
     *
     * @return the request id
     */
    public int getRequestId() {
        return requestId;
    }
}
