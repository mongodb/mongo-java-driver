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

package org.mongodb.protocol;

import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.SingleResultFuture;

public class GetMoreDiscardProtocol implements Protocol<Void> {
    private final long cursorId;
    private final int responseTo;
    private final Connection connection;

    public GetMoreDiscardProtocol(final long cursorId, final int responseTo, final Connection connection) {
        this.cursorId = cursorId;
        this.responseTo = responseTo;
        this.connection = connection;
    }

    public Void execute(final Connection connection, final ServerDescription serverDescription) {
        long curCursorId = cursorId;
        int curResponseTo = responseTo;
        while (curCursorId != 0) {
            ResponseBuffers responseBuffers = this.connection.receiveMessage(curResponseTo);
            try {
                curCursorId = responseBuffers.getReplyHeader().getCursorId();
                curResponseTo = responseBuffers.getReplyHeader().getRequestId();
            } finally {
                responseBuffers.close();
            }
        }
        return null;
    }

    public MongoFuture<Void> executeAsync(final Connection connection, final ServerDescription serverDescription) {
        SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();

        if (cursorId == 0) {
            retVal.init(null, null);
        } else {
            this.connection.receiveMessageAsync(responseTo, new DiscardCallback(retVal));
        }

        return retVal;

    }

    private class DiscardCallback implements SingleResultCallback<ResponseBuffers> {
        private final SingleResultFuture<Void> future;

        public DiscardCallback(final SingleResultFuture<Void> future) {
            this.future = future;
        }

        @Override
        public void onResult(final ResponseBuffers result, final MongoException e) {
            if (result.getReplyHeader().getCursorId() == 0) {
                future.init(null, null);
            } else {
                connection.receiveMessageAsync(responseTo, this);
            }
        }
    }

}
