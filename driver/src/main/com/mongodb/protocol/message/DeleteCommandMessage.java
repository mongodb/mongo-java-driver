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

import com.mongodb.WriteConcern;
import com.mongodb.operation.RemoveRequest;
import org.bson.BsonBinaryWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.EncoderContext;
import org.bson.io.OutputBuffer;
import org.mongodb.MongoNamespace;

import java.util.Collections;
import java.util.List;

public class DeleteCommandMessage extends BaseWriteCommandMessage {
    private final List<RemoveRequest> deletes;

    public DeleteCommandMessage(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                final List<RemoveRequest> deletes, final MessageSettings settings) {
        super(namespace, ordered, writeConcern, settings);
        this.deletes = deletes;
    }

    @Override
    public int getItemCount() {
        return deletes.size();
    }

    @Override
    protected FieldNameValidator getFieldNameValidator() {
        return new NoOpFieldNameValidator();
    }

    public List<RemoveRequest> getRequests() {
        return Collections.unmodifiableList(deletes);
    }

    @Override
    protected String getCommandName() {
        return "delete";
    }

    @Override
    protected BaseWriteCommandMessage writeTheWrites(final OutputBuffer buffer, final int commandStartPosition,
                                                     final BsonBinaryWriter writer) {
        DeleteCommandMessage nextMessage = null;
        writer.writeStartArray("deletes");
        for (int i = 0; i < deletes.size(); i++) {
            writer.mark();
            RemoveRequest removeRequest = deletes.get(i);
            writer.writeStartDocument();
            writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
            writer.writeName("q");
            getBsonDocumentCodec().encode(writer, removeRequest.getFilter(), EncoderContext.builder().build());
            writer.writeInt32("limit", removeRequest.isMulti() ? 0 : 1);
            writer.popMaxDocumentSize();
            writer.writeEndDocument();
            if (exceedsLimits(buffer.getPosition() - commandStartPosition, i + 1)) {
                writer.reset();
                nextMessage = new DeleteCommandMessage(getWriteNamespace(),
                                                       isOrdered(), getWriteConcern(), deletes.subList(i, deletes.size()),
                                                       getSettings());
                break;
            }
        }
        writer.writeEndArray();
        return nextMessage;
    }
}
