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

import com.mongodb.CommandFailureException;
import com.mongodb.MongoInternalException;
import com.mongodb.ServerAddress;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.mongodb.CommandResult;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.QueryFlag;
import org.mongodb.protocol.message.CommandMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.ReplyMessage;

import java.util.EnumSet;

import static java.lang.String.format;
import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;

final class CommandHelper {
    static CommandResult executeCommand(final String database, final BsonDocument command, final InternalConnection internalConnection) {
        return receiveCommandResult(internalConnection, sendMessage(database, command, internalConnection));
    }

    static BsonDocument executeCommandWithoutCheckingForFailure(final String database, final BsonDocument command,
                                                                final InternalConnection internalConnection) {
        return receiveCommandDocument(internalConnection, sendMessage(database, command, internalConnection));
    }

    private static CommandMessage sendMessage(final String database, final BsonDocument command,
                                              final InternalConnection internalConnection) {
        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(internalConnection);
        try {
            CommandMessage message = new CommandMessage(new MongoNamespace(database, COMMAND_COLLECTION_NAME).getFullName(),
                                                        command, EnumSet.noneOf(QueryFlag.class), MessageSettings.builder().build());
            message.encode(buffer);
            internalConnection.sendMessage(buffer.getByteBuffers(), message.getId());
            return message;
        } finally {
            buffer.close();
        }
    }

    private static CommandResult receiveCommandResult(final InternalConnection internalConnection, final CommandMessage message) {
        return createCommandResult(receiveReply(internalConnection, message), internalConnection.getServerAddress());
    }

    private static BsonDocument receiveCommandDocument(final InternalConnection internalConnection, final CommandMessage message) {
        return receiveReply(internalConnection, message).getDocuments().get(0);
    }

    private static ReplyMessage<BsonDocument> receiveReply(final InternalConnection internalConnection, final CommandMessage message) {
        ResponseBuffers responseBuffers = internalConnection.receiveMessage();
        if (responseBuffers == null) {
            throw new MongoInternalException(format("Response buffers received from %s should not be null", internalConnection));
        }
        try {
            return new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(), message.getId());
        } finally {
            responseBuffers.close();
        }
    }

    private static CommandResult createCommandResult(final ReplyMessage<BsonDocument> replyMessage, final ServerAddress serverAddress) {
        CommandResult commandResult = new CommandResult(serverAddress, replyMessage.getDocuments().get(0)
        );
        if (!commandResult.isOk()) {
            throw new CommandFailureException(commandResult.getResponse(), commandResult.getAddress());
        }

        return commandResult;
    }


    private CommandHelper() {
    }
}
