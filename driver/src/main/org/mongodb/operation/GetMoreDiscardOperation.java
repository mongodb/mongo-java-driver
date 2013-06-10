/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.operation;

import org.mongodb.Operation;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerConnection;

import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class GetMoreDiscardOperation implements Operation<Void> {
    private final long cursorId;
    private final int responseTo;

    public GetMoreDiscardOperation(final long cursorId, final int responseTo) {
        this.cursorId = cursorId;
        this.responseTo = responseTo;
    }

    public Void execute(final ServerConnection connection) {
        long curCursorId = cursorId;
        int curResponseTo = responseTo;
        while (curCursorId != 0) {
            final ResponseBuffers responseBuffers = connection.receiveMessage(
                    getResponseSettings(connection.getDescription(), curResponseTo));
            try {
                curCursorId = responseBuffers.getReplyHeader().getCursorId();
                curResponseTo = responseBuffers.getReplyHeader().getRequestId();
            } finally {
                responseBuffers.close();
            }
        }
        return null;
    }
}
