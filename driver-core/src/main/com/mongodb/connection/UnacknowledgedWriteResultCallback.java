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

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import org.bson.io.OutputBuffer;

import static com.mongodb.WriteConcern.UNACKNOWLEDGED;

class UnacknowledgedWriteResultCallback implements SingleResultCallback<Void> {
    private final SingleResultCallback<WriteConcernResult> callback;
    private final MongoNamespace namespace;
    private final RequestMessage nextMessage;
    private final OutputBuffer writtenBuffer;
    private final boolean ordered;
    private final InternalConnection connection;

    UnacknowledgedWriteResultCallback(final SingleResultCallback<WriteConcernResult> callback,
                                      final MongoNamespace namespace, final RequestMessage nextMessage,
                                      final boolean ordered, final OutputBuffer writtenBuffer,
                                      final InternalConnection connection) {
        this.callback = callback;
        this.namespace = namespace;
        this.nextMessage = nextMessage;
        this.ordered = ordered;
        this.connection = connection;
        this.writtenBuffer = writtenBuffer;
    }

    @Override
    public void onResult(final Void result, final Throwable t) {
        writtenBuffer.close();
        if (t != null) {
            callback.onResult(null, t);
        } else if (nextMessage != null) {
            new GenericWriteProtocol(namespace, nextMessage, ordered, UNACKNOWLEDGED).executeAsync(connection, callback);
        } else {
            callback.onResult(WriteConcernResult.unacknowledged(), null);
        }
    }
}
