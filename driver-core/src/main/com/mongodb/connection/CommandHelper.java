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
import com.mongodb.CursorFlag;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.protocol.message.CommandMessage;
import com.mongodb.protocol.message.MessageSettings;
import com.mongodb.protocol.message.ReplyMessage;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import java.util.EnumSet;

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static java.lang.String.format;

final class CommandHelper {
    static BsonDocument executeCommand(final String database, final BsonDocument command, final InternalConnection internalConnection) {
        return receiveCommandResult(internalConnection, sendMessage(database, command, internalConnection));
    }

    static BsonDocument executeCommandWithoutCheckingForFailure(final String database, final BsonDocument command,
                                                                final InternalConnection internalConnection) {
        return receiveCommandDocument(internalConnection, sendMessage(database, command, internalConnection));
    }

    private static CommandMessage sendMessage(final String database, final BsonDocument command,
                                              final InternalConnection internalConnection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(internalConnection);
        try {
            CommandMessage message = new CommandMessage(new MongoNamespace(database, COMMAND_COLLECTION_NAME).getFullName(),
                                                        command, EnumSet.noneOf(CursorFlag.class), MessageSettings.builder().build());
            message.encode(bsonOutput);
            internalConnection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return message;
        } finally {
            bsonOutput.close();
        }
    }

    private static BsonDocument receiveCommandResult(final InternalConnection internalConnection, final CommandMessage message) {
        BsonDocument result = receiveReply(internalConnection, message).getDocuments().get(0);
        if (!isCommandOk(result)) {
            throw new CommandFailureException(result, internalConnection.getServerAddress());
        }

        return result;
    }

    private static BsonDocument receiveCommandDocument(final InternalConnection internalConnection, final CommandMessage message) {
        return receiveReply(internalConnection, message).getDocuments().get(0);
    }

    private static ReplyMessage<BsonDocument> receiveReply(final InternalConnection internalConnection, final CommandMessage message) {
        ResponseBuffers responseBuffers = internalConnection.receiveMessage(message.getId());
        if (responseBuffers == null) {
            throw new MongoInternalException(format("Response buffers received from %s should not be null", internalConnection));
        }
        try {
            return new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(), message.getId());
        } finally {
            responseBuffers.close();
        }
    }

    private static boolean isCommandOk(final BsonDocument response) {
        BsonValue okValue = response.get("ok");
        if (okValue.isBoolean()) {
            return okValue.asBoolean().getValue();
        } else if (okValue.isNumber()) {
            return okValue.asNumber().intValue() == 1;
        } else {
            return false;
        }
    }

    private CommandHelper() {
    }
}
