/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.mongodb.protocol;


import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.ServerCursor;
import org.mongodb.connection.ServerAddress;
import org.mongodb.protocol.message.ReplyMessage;

import java.util.List;

public class QueryResult<T> {
    private final List<T> results;
    private final ServerCursor serverCursor;
    private final ServerAddress serverAddress;
    private final int requestId;

    public QueryResult(final ReplyMessage<T> replyMessage, final ServerAddress address) {
        if (replyMessage.getReplyHeader().getCursorId() != 0) {
            serverCursor = new ServerCursor(replyMessage.getReplyHeader().getCursorId(), address);
        } else {
            serverCursor = null;
        }
        serverAddress = address;
        results = replyMessage.getDocuments();
        requestId = replyMessage.getReplyHeader().getRequestId();
    }

    @SuppressWarnings("unchecked")
    public QueryResult(final CommandResult result, final ServerAddress address) {
        Document cursor = (Document) result.getResponse().get("cursor");
        if (cursor != null) {
            if (cursor.getLong("id") != 0) {
                serverCursor = new ServerCursor(cursor.getLong("id"), address);
            } else {
                serverCursor = null;
            }
            results = (List<T>) cursor.get("firstBatch");
        } else {
            serverCursor = null;
            results = (List<T>) result.getResponse().get("result");
        }
        serverAddress = address;
        requestId = 0;
    }

    public ServerCursor getCursor() {
        return serverCursor;
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
