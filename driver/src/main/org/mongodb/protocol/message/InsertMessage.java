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
import org.mongodb.Encoder;
import org.mongodb.WriteConcern;
import org.mongodb.operation.InsertRequest;

import java.util.List;

public class InsertMessage<T> extends RequestMessage {

    private final boolean ordered;
    private final WriteConcern writeConcern;
    private final List<InsertRequest<T>> insertRequestList;
    private final Encoder<T> encoder;

    public InsertMessage(final String collectionName, final boolean ordered, final WriteConcern writeConcern,
                         final List<InsertRequest<T>> insertRequestList, final Encoder<T> encoder, final MessageSettings settings) {
        super(collectionName, OpCode.OP_INSERT, settings);
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.insertRequestList = insertRequestList;
        this.encoder = encoder;
    }

    @Override
    protected RequestMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        writeInsertPrologue(buffer);
        for (int i = 0; i < insertRequestList.size(); i++) {
            T document = insertRequestList.get(i).getDocument();
            int pos = buffer.getPosition();
            addDocument(document, encoder, buffer);
            if (buffer.getPosition() - messageStartPosition > getSettings().getMaxMessageSize()) {
                buffer.truncateToPosition(pos);
                return new InsertMessage<T>(getCollectionName(), ordered, writeConcern,
                                            insertRequestList.subList(i, insertRequestList.size()), encoder, getSettings());
            }
        }
        return null;
    }

    private void writeInsertPrologue(final OutputBuffer buffer) {
        int flags = 0;
        if (!ordered) {
            flags |= 1;
        }
        buffer.writeInt(flags);
        buffer.writeCString(getCollectionName());
    }
}
