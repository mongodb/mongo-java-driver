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

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.protocol.message.ReplyMessage;

abstract class CommandResultBaseCallback extends ResponseCallback {
    private final Decoder<Document> decoder;

    public CommandResultBaseCallback(final Decoder<Document> decoder, final long requestId, final Connection connection,
                                     final boolean closeConnection) {
        super(requestId, connection, closeConnection);
        this.decoder = decoder;
    }

    protected boolean callCallback(final ResponseBuffers responseBuffers, final MongoException e) {
        try {
            if (e != null || responseBuffers == null) {
                return callCallback((CommandResult) null, e);
            }
            else {
                ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, decoder, getRequestId());
                return callCallback(new CommandResult(getConnection().getServerAddress(), replyMessage.getDocuments().get(0),
                        replyMessage.getElapsedNanoseconds()), null);
            }
        } finally {
            if (responseBuffers != null) {
                responseBuffers.close();
            }
        }
    }

    protected abstract boolean callCallback(final CommandResult commandResult, final MongoException e);
}
