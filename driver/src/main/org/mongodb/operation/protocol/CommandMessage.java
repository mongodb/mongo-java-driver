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

package org.mongodb.operation.protocol;

import org.bson.io.OutputBuffer;
import org.mongodb.Document;
import org.mongodb.Encoder;

import static org.mongodb.operation.protocol.RequestMessage.OpCode.OP_QUERY;

public class CommandMessage extends RequestMessage {
    private final Encoder<Document> encoder;
    private final Document command;

    public CommandMessage(final String collectionName, final Document command, final Encoder<Document> encoder,
                          final MessageSettings settings) {
        super(collectionName, OP_QUERY, settings);
        this.encoder = encoder;
        this.command = command;
    }

    @Override
    protected RequestMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        buffer.writeInt(0);
        buffer.writeCString(getCollectionName());

        buffer.writeInt(0);
        buffer.writeInt(-1);
        addDocument(command, encoder, buffer);
        return null;
    }
}
