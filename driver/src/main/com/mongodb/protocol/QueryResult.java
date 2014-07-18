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
import com.mongodb.protocol.message.ReplyMessage;
import org.mongodb.ServerCursor;

import java.util.List;

public class QueryResult<T> {
    private final List<T> results;
    private final long cursorId;
    private final ServerAddress serverAddress;
    private final int requestId;

    public QueryResult(final List<T> results, final long cursorId, final ServerAddress serverAddress, final int requestId) {
        this.results = results;
        this.cursorId = cursorId;
        this.serverAddress = serverAddress;
        this.requestId = requestId;
    }

    QueryResult(final ReplyMessage<T> replyMessage, final ServerAddress address) {
        this(replyMessage.getDocuments(), replyMessage.getReplyHeader().getCursorId(), address,
             replyMessage.getReplyHeader().getRequestId());
    }

    public ServerCursor getCursor() {
        return cursorId == 0 ? null : new ServerCursor(cursorId, serverAddress);
    }

    public List<T> getResults() {
        return results;
    }

    public ServerAddress getAddress() {
        return serverAddress;
    }

    public int getRequestId() {
        return requestId;
    }

}
