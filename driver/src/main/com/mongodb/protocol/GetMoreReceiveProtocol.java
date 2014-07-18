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

import com.mongodb.connection.Connection;
import com.mongodb.connection.ResponseBuffers;
import com.mongodb.protocol.message.ReplyMessage;
import org.bson.codecs.Decoder;
import org.mongodb.MongoFuture;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;

public class GetMoreReceiveProtocol<T> implements Protocol<QueryResult<T>> {

    private final Decoder<T> resultDecoder;
    private final int responseTo;

    public GetMoreReceiveProtocol(final Decoder<T> resultDecoder, final int responseTo) {
        this.resultDecoder = resultDecoder;
        this.responseTo = responseTo;
    }

    public QueryResult<T> execute(final Connection connection) {
        ResponseBuffers responseBuffers = connection.receiveMessage(responseTo);
        try {
            return new QueryResult<T>(new ReplyMessage<T>(responseBuffers, resultDecoder, responseTo), connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }

    public MongoFuture<QueryResult<T>> executeAsync(final Connection connection) {
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        connection.receiveMessageAsync(responseTo, new GetMoreResultCallback<T>(new SingleResultFutureCallback<QueryResult<T>>(retVal),
                                                                                resultDecoder, 0, responseTo,
                                                                                connection.getServerAddress()));

        return retVal;
    }
}
