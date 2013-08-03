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

import org.mongodb.AsyncOperation;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.SingleResultCallback;

import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class AsyncGetMoreDiscardOperation implements AsyncOperation<Void> {
    private final long cursorId;
    private final int responseTo;

    public AsyncGetMoreDiscardOperation(final long cursorId, final int responseTo) {
        this.cursorId = cursorId;
        this.responseTo = responseTo;
    }

    @Override
    public MongoFuture<Void> execute(final AsyncServerConnection connection) {
        final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();

        if (cursorId == 0) {
            retVal.init(null, null);
        }
        else {
            connection.receiveMessage(getResponseSettings(connection.getDescription(), responseTo),
                    new DiscardCallback(connection, retVal));
        }

        return retVal;

    }

    private class DiscardCallback implements SingleResultCallback<ResponseBuffers> {

        private AsyncServerConnection connection;
        private SingleResultFuture<Void> future;

        public DiscardCallback(final AsyncServerConnection connection, final SingleResultFuture<Void> future) {
            this.connection = connection;
            this.future = future;
        }

        @Override
        public void onResult(final ResponseBuffers result, final MongoException e) {
            if (result.getReplyHeader().getCursorId() == 0) {
                future.init(null, null);
            }
            else {
                connection.receiveMessage(getResponseSettings(connection.getDescription(), responseTo),
                        new DiscardCallback(connection, future));
            }
        }
    }
}
