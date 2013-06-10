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

import org.mongodb.Decoder;
import org.mongodb.Operation;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerConnection;
import org.mongodb.operation.protocol.ReplyMessage;

import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class GetMoreReceiveOperation<T> implements Operation<QueryResult<T>> {

    private final Decoder<T> resultDecoder;
    private final int responseTo;

    public GetMoreReceiveOperation(final Decoder<T> resultDecoder, final int responseTo) {
        this.resultDecoder = resultDecoder;
        this.responseTo = responseTo;
    }

    public QueryResult<T> execute(final ServerConnection connection) {
        final ResponseBuffers responseBuffers = connection.receiveMessage(getResponseSettings(connection.getDescription(), responseTo));
        try {
            return new QueryResult<T>(new ReplyMessage<T>(responseBuffers, resultDecoder, responseTo), connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }
}
