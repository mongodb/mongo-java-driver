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

package org.mongodb.protocol.message;

import org.bson.io.OutputBuffer;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.operation.QueryFlag;

import java.util.EnumSet;

import static org.mongodb.protocol.message.RequestMessage.OpCode.OP_QUERY;

public class CommandMessage extends RequestMessage {
    private final EnumSet<QueryFlag> queryFlags;
    private final Encoder<Document> encoder;
    private final Document command;

    public CommandMessage(final String collectionName, final Document command, final EnumSet<QueryFlag> queryFlags,
                          final Encoder<Document> encoder, final MessageSettings settings) {
        super(collectionName, OP_QUERY, settings);
        this.queryFlags = queryFlags;
        this.encoder = encoder;
        this.command = command;
    }

    @Override
    protected RequestMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        buffer.writeInt(QueryFlag.fromSet(queryFlags));
        buffer.writeCString(getCollectionName());

        buffer.writeInt(0);
        buffer.writeInt(-1);
        addDocument(command, encoder, buffer);
        return null;
    }
}
