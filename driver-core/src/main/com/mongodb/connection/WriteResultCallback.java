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
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernException;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;

class WriteResultCallback extends CommandResultBaseCallback<BsonDocument> {
    private final SingleResultCallback<WriteConcernResult> callback;
    private final MongoNamespace namespace;
    private final RequestMessage nextMessage; // only used for batch inserts that need to be split into multiple messages
    private final boolean ordered;
    private final WriteConcern writeConcern;
    private final InternalConnection connection;

    // CHECKSTYLE:OFF
    public WriteResultCallback(final SingleResultCallback<WriteConcernResult> callback, final Decoder<BsonDocument> decoder,
                               final MongoNamespace namespace, final RequestMessage nextMessage,
                               final boolean ordered, final WriteConcern writeConcern, final long requestId,
                               final InternalConnection connection) {
        // CHECKSTYLE:ON
        super(decoder, requestId, connection.getDescription().getServerAddress());
        this.callback = callback;
        this.namespace = namespace;
        this.nextMessage = nextMessage;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.connection = connection;
    }

    @Override
    protected void callCallback(final BsonDocument result, final Throwable t) {
        if (t != null) {
            callback.onResult(null, t);
        } else {
            try {
                WriteConcernResult writeConcernResult = null;
                boolean shouldWriteNextMessage = true;
                try {
                    writeConcernResult = ProtocolHelper.getWriteResult(result, connection.getDescription().getServerAddress());
                } catch (WriteConcernException e) {
                    if (writeConcern.isAcknowledged()) {
                        throw e;
                    }
                    if (ordered) {
                        shouldWriteNextMessage = false;
                    }
                }
                if (shouldWriteNextMessage && nextMessage != null) {
                    new GenericWriteProtocol(namespace, nextMessage, ordered, writeConcern).executeAsync(connection, callback);
                } else {
                    callback.onResult(writeConcernResult, null);
                }
            } catch (Throwable t1) {
                callback.onResult(null, t1);
            }
        }
    }
}
