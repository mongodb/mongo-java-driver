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
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.protocol.MessageSettings;
import org.mongodb.operation.protocol.ReplyMessage;
import org.mongodb.operation.protocol.RequestMessage;

// TODO: these should move somewhere else.
public final class OperationHelpers {

    public static CommandResult createCommandResult(final ReplyMessage<Document> replyMessage, final ServerAddress serverAddress) {
        CommandResult commandResult = new CommandResult(serverAddress, replyMessage.getDocuments().get(0),
                replyMessage.getElapsedNanoseconds());
        if (!commandResult.isOk()) {
            throw new MongoCommandFailureException(commandResult);
        }

        return commandResult;
    }

    public static MessageSettings getMessageSettings(final ServerDescription serverDescription) {
        return MessageSettings.builder().maxDocumentSize(serverDescription.getMaxDocumentSize()).maxMessageSize(serverDescription
                .getMaxMessageSize()).build();
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
