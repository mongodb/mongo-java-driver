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

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.operation.InsertRequest;
import org.bson.BsonBinaryWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.io.OutputBuffer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertCommandMessage<T> extends BaseWriteCommandMessage {
    private final List<InsertRequest<T>> insertRequestList;
    private final Encoder<T> encoder;

    public InsertCommandMessage(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                final List<InsertRequest<T>> insertRequestList,
                                final Encoder<T> encoder, final MessageSettings settings) {
        super(namespace, ordered, writeConcern, settings);
        this.insertRequestList = insertRequestList;
        this.encoder = encoder;
    }

    @Override
    public int getItemCount() {
        return insertRequestList.size();
    }

    @Override
    protected FieldNameValidator getFieldNameValidator() {
        Map<String, FieldNameValidator> map = new HashMap<String, FieldNameValidator>();
        map.put("documents", new CollectibleDocumentFieldNameValidator());
        return new MappedFieldNameValidator(new NoOpFieldNameValidator(), map);
    }

    public List<InsertRequest<T>> getRequests() {
        return Collections.unmodifiableList(insertRequestList);
    }

    protected String getCommandName() {
        return "insert";
    }

    protected InsertCommandMessage<T> writeTheWrites(final OutputBuffer buffer, final int commandStartPosition,
                                                     final BsonBinaryWriter writer) {
        InsertCommandMessage<T> nextMessage = null;
        writer.writeStartArray("documents");
        writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
        for (int i = 0; i < insertRequestList.size(); i++) {
            writer.mark();
            encoder.encode(writer,
                           insertRequestList.get(i).getDocument(),
                           EncoderContext.builder().isEncodingCollectibleDocument(true).build());
            if (exceedsLimits(buffer.getPosition() - commandStartPosition, i + 1)) {
                writer.reset();
                nextMessage = new InsertCommandMessage<T>(getWriteNamespace(), isOrdered(), getWriteConcern(),
                                                          insertRequestList.subList(i, insertRequestList.size()),
                                                          encoder, getSettings());
                break;
            }
        }
        writer.popMaxDocumentSize();
        writer.writeEndArray();
        return nextMessage;
    }
}
