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
import org.mongodb.Decoder;
import org.mongodb.MongoFuture;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.operation.protocol.QueryResult;

import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class AsyncGetMoreReceiveOperation<T> implements AsyncOperation<QueryResult<T>> {

    private final Decoder<T> resultDecoder;
    private final int responseTo;

    public AsyncGetMoreReceiveOperation(final Decoder<T> resultDecoder, final int responseTo) {
        this.resultDecoder = resultDecoder;
        this.responseTo = responseTo;
    }

    @Override
    public MongoFuture<QueryResult<T>> execute(final AsyncServerConnection connection) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        connection.receiveMessage(getResponseSettings(connection.getDescription(), responseTo), new GetMoreResultCallback<T>(
                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, 0, connection, responseTo));

        return retVal;

    }
}
