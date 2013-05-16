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

import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.connection.ChannelAwareOutputBuffer;
import org.mongodb.operation.MongoRemove;

public class MongoDeleteMessage extends MongoRequestMessage {
    private final MongoRemove remove;
    private final Encoder<Document> encoder;

    public MongoDeleteMessage(final String collectionName, final MongoRemove remove, final Encoder<Document> encoder) {
        super(collectionName, OpCode.OP_DELETE);
        this.remove = remove;
        this.encoder = encoder;
    }

    @Override
    protected MongoRequestMessage encodeMessageBody(final ChannelAwareOutputBuffer buffer, final int messageStartPosition) {
        writeDelete(buffer);
        return null;
    }

    private void writeDelete(final ChannelAwareOutputBuffer buffer) {
        buffer.writeInt(0); // reserved
        buffer.writeCString(getCollectionName());

        if (remove.isMulti()) {
            buffer.writeInt(0);
        }
        else {
            buffer.writeInt(1);
        }

        addDocument(remove.getFilter(), encoder, buffer);
    }
}

