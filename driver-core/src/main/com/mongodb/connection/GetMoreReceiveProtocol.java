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

package com.mongodb.connection;

import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultFuture;
import org.bson.codecs.Decoder;

/**
 * An implementation of the OP_GET_MORE protocol that can be used to receive the next batch of documents from an exhaust cursor.
 *
 * @param <T> the type of document to decode query results to
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-get-more OP_GET_MORE
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 * @since 3.0
 */
public class GetMoreReceiveProtocol<T> implements Protocol<QueryResult<T>> {

    private final Decoder<T> resultDecoder;
    private final int responseTo;

    /**
     * Construct an instance.
     *
     * @param resultDecoder the decoder for the result documents
     * @param responseTo    the expected responseTo field for next batch
     */
    public GetMoreReceiveProtocol(final Decoder<T> resultDecoder, final int responseTo) {
        this.resultDecoder = resultDecoder;
        this.responseTo = responseTo;
    }

    @Override
    public QueryResult<T> execute(final Connection connection) {
        ResponseBuffers responseBuffers = connection.receiveMessage(responseTo);
        try {
            return new QueryResult<T>(new ReplyMessage<T>(responseBuffers, resultDecoder, responseTo), connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }

    @Override
    public MongoFuture<QueryResult<T>> executeAsync(final Connection connection) {
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        connection.receiveMessageAsync(responseTo, new GetMoreResultCallback<T>(new SingleResultFutureCallback<QueryResult<T>>(retVal),
                                                                                resultDecoder, 0, responseTo,
                                                                                connection.getServerAddress()));

        return retVal;
    }
}
