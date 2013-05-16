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

import org.mongodb.Encoder;
import org.mongodb.WriteConcern;
import org.mongodb.connection.ChannelAwareOutputBuffer;
import org.mongodb.operation.MongoInsert;

public class MongoInsertMessage<T> extends MongoRequestMessage {
    private static final int MAX_MESSAGE_SIZE = 48000000;  // TODO: this shouldn't be a constant

    private final MongoInsert<T> insert;
    private final Encoder<T> encoder;

    public MongoInsertMessage(final String collectionName, final MongoInsert<T> insert, final Encoder<T> encoder) {
        super(collectionName, OpCode.OP_INSERT);
        this.insert = insert;
        this.encoder = encoder;
    }

    @Override
    protected MongoRequestMessage encodeMessageBody(final ChannelAwareOutputBuffer buffer, final int messageStartPosition) {
        writeInsertPrologue(insert.getWriteConcern(), buffer);
        for (int i = 0; i < insert.getDocuments().size(); i++) {
            T document = insert.getDocuments().get(i);
            int pos = buffer.getPosition();
            addDocument(document, encoder, buffer);
            if (buffer.getPosition() - messageStartPosition > MAX_MESSAGE_SIZE) {
                buffer.truncateToPosition(pos);
                return new MongoInsertMessage<T>(getCollectionName(), new MongoInsert<T>(insert, i), encoder);
            }
        }
        return null;
    }

    private void writeInsertPrologue(final WriteConcern concern, final ChannelAwareOutputBuffer buffer) {
        int flags = 0;
        if (concern.getContinueOnErrorForInsert()) {
            flags |= 1;
        }
        buffer.writeInt(flags);
        buffer.writeCString(getCollectionName());
    }
}
