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

package org.mongodb.protocol;

import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.mongodb.CommandResult;
import org.mongodb.MongoException;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerAddress;
import org.mongodb.protocol.message.ReplyMessage;

abstract class CommandResultBaseCallback<T> extends ResponseCallback {
    private final Decoder<T> decoder;
    private final Decoder<BsonDocument> rawDecoder;

    public CommandResultBaseCallback(final Decoder<T> decoder, final Decoder<BsonDocument> rawDecoder,
                                     final long requestId, final ServerAddress serverAddress) {
        super(requestId, serverAddress);
        this.decoder = decoder;
        this.rawDecoder = rawDecoder;
    }

    protected boolean callCallback(final ResponseBuffers responseBuffers, final MongoException e) {
        try {
            if (e != null || responseBuffers == null) {
                return callCallback((CommandResult<T>) null, e);
            } else {
                ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, rawDecoder, getRequestId());
                return callCallback(new CommandResult<T>(getServerAddress(), replyMessage.getDocuments().get(0),
                                    replyMessage.getElapsedNanoseconds(), decoder), null);
            }
        } finally {
            if (responseBuffers != null) {
                responseBuffers.close();
            }
        }
    }

    protected abstract boolean callCallback(CommandResult<T> commandResult, MongoException e);
}
