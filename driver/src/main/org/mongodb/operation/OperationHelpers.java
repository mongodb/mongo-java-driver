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
import org.mongodb.Document;
import org.mongodb.command.Command;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.protocol.MessageSettings;
import org.mongodb.operation.protocol.ReplyMessage;
import org.mongodb.operation.protocol.RequestMessage;

// TODO: these should move somewhere else.
public final class OperationHelpers {

    public static CommandResult createCommandResult(final Command commandOperation, final ReplyMessage<Document> replyMessage,
                                                    final Connection connection) {
        CommandResult commandResult = new CommandResult(commandOperation.toDocument(), connection.getServerAddress(),
                replyMessage.getDocuments().get(0), replyMessage.getElapsedNanoseconds());
        if (!commandResult.isOk()) {
            throw new MongoCommandFailureException(commandResult);
        }

        return commandResult;
    }

    public static MessageSettings getMessageSettings(final ServerDescription serverDescription) {
        return MessageSettings.builder().maxDocumentSize(serverDescription.getMaxDocumentSize()).maxMessageSize(serverDescription
                .getMaxMessageSize()).build();
    }

    public static ResponseSettings getResponseSettings(final ServerDescription serverDescription, final int responseTo) {
        return ResponseSettings.builder().maxMessageSize(serverDescription.getMaxMessageSize()).responseTo(responseTo).build();
    }


    public static RequestMessage encodeMessageToBuffer(final RequestMessage message, final PooledByteBufferOutputBuffer buffer) {
        try {
            return message.encode(buffer);
        } catch (RuntimeException e) {
            buffer.close();
            throw e;
        } catch (Error e) {
            buffer.close();
            throw e;
        }
    }

    private OperationHelpers() {
    }
}
