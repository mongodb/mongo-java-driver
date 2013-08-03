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

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.command.Command;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.operation.protocol.ReplyMessage;

abstract class CommandResultBaseCallback extends ResponseCallback {
    private final Command commandOperation;
    private final Decoder<Document> decoder;

    public CommandResultBaseCallback(final Command commandOperation, final Decoder<Document> decoder,
                                     final AsyncServerConnection connection, final long requestId) {
        super(connection, requestId);
        this.commandOperation = commandOperation;
        this.decoder = decoder;
    }

    protected void callCallback(final ResponseBuffers responseBuffers, final MongoException e) {
        try {
            if (e != null || responseBuffers == null) {
                callCallback((CommandResult) null, e);
            }
            else {
                ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, decoder, getRequestId());
                callCallback(new CommandResult(commandOperation.toDocument(), getConnection().getServerAddress(),
                        replyMessage.getDocuments().get(0), replyMessage.getElapsedNanoseconds()), null);
            }
        } finally {
            if (responseBuffers != null) {
                responseBuffers.close();
            }
        }
    }

    protected abstract void callCallback(final CommandResult commandResult, final MongoException e);
}
