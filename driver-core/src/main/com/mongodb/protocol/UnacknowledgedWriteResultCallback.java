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

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.connection.Connection;
import com.mongodb.protocol.message.RequestMessage;
import org.bson.io.OutputBuffer;

import static com.mongodb.WriteConcern.UNACKNOWLEDGED;

class UnacknowledgedWriteResultCallback implements SingleResultCallback<Void> {
    private final SingleResultFuture<WriteConcernResult> future;
    private final MongoNamespace namespace;
    private final RequestMessage nextMessage;
    private final OutputBuffer writtenBuffer;
    private final boolean ordered;
    private final Connection connection;

    UnacknowledgedWriteResultCallback(final SingleResultFuture<WriteConcernResult> future,
                                      final MongoNamespace namespace, final RequestMessage nextMessage,
                                      final boolean ordered, final OutputBuffer writtenBuffer,
                                      final Connection connection) {
        this.future = future;
        this.namespace = namespace;
        this.nextMessage = nextMessage;
        this.ordered = ordered;
        this.connection = connection;
        this.writtenBuffer = writtenBuffer;
    }

    @Override
    public void onResult(final Void result, final MongoException e) {
        writtenBuffer.close();
        if (e != null) {
            future.init(null, e);
        } else if (nextMessage != null) {
            MongoFuture<WriteConcernResult> newFuture = new GenericWriteProtocol(namespace, nextMessage, ordered, UNACKNOWLEDGED)
                                                 .executeAsync(connection);
            newFuture.register(new SingleResultFutureCallback<WriteConcernResult>(future));
        } else {
            future.init(new UnacknowledgedWriteConcernResult(), null);
        }
    }
}
