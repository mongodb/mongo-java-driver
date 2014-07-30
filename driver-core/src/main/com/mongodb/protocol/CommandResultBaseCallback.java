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
import com.mongodb.ServerAddress;
import com.mongodb.connection.ResponseBuffers;
import com.mongodb.protocol.message.ReplyMessage;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.mongodb.CommandResult;

abstract class CommandResultBaseCallback extends ResponseCallback {
    private final Decoder<BsonDocument> decoder;

    public CommandResultBaseCallback(final Decoder<BsonDocument> decoder, final long requestId, final ServerAddress serverAddress) {
        super(requestId, serverAddress);
        this.decoder = decoder;
    }

    protected boolean callCallback(final ResponseBuffers responseBuffers, final MongoException e) {
        try {
            if (e != null || responseBuffers == null) {
                return callCallback((CommandResult) null, e);
            } else {
                ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, decoder, getRequestId());
                return callCallback(new CommandResult(getServerAddress(), replyMessage.getDocuments().get(0)
                ), null);
            }
        } finally {
            if (responseBuffers != null) {
                responseBuffers.close();
            }
        }
    }

    protected abstract boolean callCallback(CommandResult commandResult, MongoException e);
}
