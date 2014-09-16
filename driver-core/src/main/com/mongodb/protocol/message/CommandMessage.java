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

package com.mongodb.protocol.message;

import com.mongodb.CursorFlag;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.io.BsonOutputStream;

import java.util.EnumSet;

import static com.mongodb.protocol.message.RequestMessage.OpCode.OP_QUERY;

public class CommandMessage extends RequestMessage {
    private final EnumSet<CursorFlag> cursorFlags;
    private final BsonDocument command;
    private final FieldNameValidator validator;

    public CommandMessage(final String collectionName, final BsonDocument command, final EnumSet<CursorFlag> cursorFlags,
                          final MessageSettings settings) {
        this(collectionName, command, cursorFlags, new NoOpFieldNameValidator(), settings);
    }

    public CommandMessage(final String collectionName, final BsonDocument command, final EnumSet<CursorFlag> cursorFlags,
                          final FieldNameValidator validator, final MessageSettings settings) {
        super(collectionName, OP_QUERY, settings);
        this.cursorFlags = cursorFlags;
        this.command = command;
        this.validator = validator;
    }

    @Override
    protected RequestMessage encodeMessageBody(final BsonOutputStream outputStream, final int messageStartPosition) {
        outputStream.writeInt32(CursorFlag.fromSet(cursorFlags));
        outputStream.writeCString(getCollectionName());

        outputStream.writeInt32(0);
        outputStream.writeInt32(-1);
        addDocument(command, getBsonDocumentCodec(), outputStream, validator);
        return null;
    }
}
