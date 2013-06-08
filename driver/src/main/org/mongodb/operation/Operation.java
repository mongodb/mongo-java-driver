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

import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.command.Command;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.protocol.MessageSettings;
import org.mongodb.operation.protocol.ReplyMessage;

public abstract class Operation {
    private MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public Operation(final MongoNamespace namespace, final BufferProvider bufferProvider) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public BufferProvider getBufferProvider() {
        return bufferProvider;
    }

    // TODO: this should move somewhere else.
    protected CommandResult createCommandResult(final Command commandOperation, final ReplyMessage<Document> replyMessage,
                                              final Connection connection) {
        CommandResult commandResult = new CommandResult(commandOperation.toDocument(), connection.getServerAddress(),
                replyMessage.getDocuments().get(0), replyMessage.getElapsedNanoseconds());
        if (!commandResult.isOk()) {
            throw new MongoCommandFailureException(commandResult);
        }

        return commandResult;
    }

    protected static MessageSettings getMessageSettings(final ServerDescription serverDescription) {
        return MessageSettings.builder().maxDocumentSize(serverDescription.getMaxDocumentSize()).maxMessageSize(serverDescription
                .getMaxMessageSize()).build();
    }

    protected static ResponseSettings getResponseSettings(final ServerDescription serverDescription, final int responseTo) {
        return ResponseSettings.builder().maxMessageSize(serverDescription.getMaxMessageSize()).responseTo(responseTo).build();
    }
}
