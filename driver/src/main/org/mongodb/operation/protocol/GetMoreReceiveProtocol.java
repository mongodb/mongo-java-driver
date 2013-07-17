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

package org.mongodb.operation.protocol;

import org.mongodb.Decoder;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.QueryResult;

import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class GetMoreReceiveProtocol<T> implements Protocol<QueryResult<T>> {

    private final Decoder<T> resultDecoder;
    private final int responseTo;
    private final ServerDescription serverDescription;
    private final Connection connection;

    public GetMoreReceiveProtocol(final Decoder<T> resultDecoder, final int responseTo, final ServerDescription serverDescription,
                                  final Connection connection) {
        this.resultDecoder = resultDecoder;
        this.responseTo = responseTo;
        this.serverDescription = serverDescription;
        this.connection = connection;
    }

    public QueryResult<T> execute() {
        final ResponseBuffers responseBuffers = connection.receiveMessage(getResponseSettings(serverDescription, responseTo));
        try {
            return new QueryResult<T>(new ReplyMessage<T>(responseBuffers, resultDecoder, responseTo), connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }
}
