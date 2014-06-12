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

package org.mongodb.connection;

import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.mongodb.CommandResult;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.QueryFlag;
import org.mongodb.protocol.message.CommandMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.ReplyMessage;

import java.util.EnumSet;

import static java.lang.String.format;
import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;

final class CommandHelper {
    static CommandResult executeCommand(final String database, final BsonDocument command,
                                        final InternalConnection internalConnection) {
        return receiveMessage(internalConnection, sendMessage(database, command, internalConnection));
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

    private static CommandResult receiveMessage(final InternalConnection internalConnection, final CommandMessage message) {
        ResponseBuffers responseBuffers = internalConnection.receiveMessage();
        if (responseBuffers == null) {
            throw new MongoInternalException(format("Response buffers received from %s should not be null", internalConnection));
        }
        try {
            ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers,
                                                                                     new BsonDocumentCodec(),
                                                                                     message.getId());
            return createCommandResult(replyMessage, internalConnection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }

    private static CommandResult createCommandResult(final ReplyMessage<BsonDocument> replyMessage, final ServerAddress serverAddress) {
        CommandResult commandResult = new CommandResult(serverAddress, replyMessage.getDocuments().get(0),
                                                        replyMessage.getElapsedNanoseconds());
        if (!commandResult.isOk()) {
            throw new MongoCommandFailureException(commandResult);
        }

        return commandResult;
    }


    private CommandHelper() {
    }
}
