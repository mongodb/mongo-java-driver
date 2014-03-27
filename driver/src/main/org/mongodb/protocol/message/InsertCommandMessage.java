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

import org.bson.BSONBinaryWriter;
import org.bson.io.OutputBuffer;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.operation.InsertRequest;

import java.util.Collections;
import java.util.List;

public class InsertCommandMessage<T> extends BaseWriteCommandMessage {
    private final List<InsertRequest<T>> insertRequestList;
    private final Encoder<T> encoder;

    public InsertCommandMessage(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                final List<InsertRequest<T>> insertRequestList, final Encoder<Document> commandEncoder,
                                final Encoder<T> encoder, final MessageSettings settings) {
        super(namespace, ordered, writeConcern, commandEncoder, settings);
        this.insertRequestList = insertRequestList;
        this.encoder = encoder;
    }

    @Override
    public int getItemCount() {
        return insertRequestList.size();
    }

    public List<InsertRequest<T>> getRequests() {
        return Collections.unmodifiableList(insertRequestList);
    }

    protected String getCommandName() {
        return "insert";
    }

    protected InsertCommandMessage<T> writeTheWrites(final OutputBuffer buffer, final int commandStartPosition,
                                                     final BSONBinaryWriter writer) {
        InsertCommandMessage<T> nextMessage = null;
        writer.writeStartArray("documents");
        writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
        for (int i = 0; i < insertRequestList.size(); i++) {
            writer.mark();
            encoder.encode(writer, insertRequestList.get(i).getDocument());
            if (exceedsLimits(buffer.getPosition() - commandStartPosition, i + 1)) {
                writer.reset();
                nextMessage = new InsertCommandMessage<T>(getWriteNamespace(), isOrdered(), getWriteConcern(),
                                                          insertRequestList.subList(i, insertRequestList .size()),
                                                          getCommandEncoder(), encoder, getSettings());
                break;
            }
        }
        writer.popMaxDocumentSize();
        writer.writeEndArray();
        return nextMessage;
    }
}
