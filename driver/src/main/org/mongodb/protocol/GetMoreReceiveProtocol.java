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

package org.mongodb.protocol;

import org.mongodb.Decoder;
import org.mongodb.MongoFuture;
import org.mongodb.connection.Channel;
import org.mongodb.connection.ChannelReceiveArgs;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;
import org.mongodb.protocol.message.ReplyMessage;

public class GetMoreReceiveProtocol<T> implements Protocol<QueryResult<T>> {

    private final Decoder<T> resultDecoder;
    private final int responseTo;
    private final Channel channel;

    public GetMoreReceiveProtocol(final Decoder<T> resultDecoder, final int responseTo, final Channel channel) {
        this.resultDecoder = resultDecoder;
        this.responseTo = responseTo;
        this.channel = channel;
    }

    public QueryResult<T> execute() {
        final ResponseBuffers responseBuffers = channel.receiveMessage(new ChannelReceiveArgs(responseTo));
        try {
            return new QueryResult<T>(new ReplyMessage<T>(responseBuffers, resultDecoder, responseTo), channel.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }

    public MongoFuture<QueryResult<T>> executeAsync() {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        channel.receiveMessageAsync(new ChannelReceiveArgs(responseTo), new GetMoreResultCallback<T>(
                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, 0, responseTo, channel, false));

        return retVal;
    }
}
