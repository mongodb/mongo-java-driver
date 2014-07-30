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
import com.mongodb.operation.BaseUpdateRequest;
import org.bson.BsonBinaryWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.EncoderContext;
import org.bson.io.OutputBuffer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseUpdateCommandMessage<T extends BaseUpdateRequest> extends BaseWriteCommandMessage {
    private final List<T> updates;

    public BaseUpdateCommandMessage(final MongoNamespace writeNamespace, final boolean ordered, final WriteConcern writeConcern,
                                    final List<T> updates, final MessageSettings settings) {
        super(writeNamespace, ordered, writeConcern, settings);
        this.updates = updates;
    }

    public List<T> getRequests() {
        return Collections.unmodifiableList(updates);
    }

    @Override
    protected BaseUpdateCommandMessage<T> writeTheWrites(final OutputBuffer buffer, final int commandStartPosition,
                                                         final BsonBinaryWriter writer) {
        BaseUpdateCommandMessage<T> nextMessage = null;
        writer.writeStartArray("updates");
        for (int i = 0; i < updates.size(); i++) {
            writer.mark();
            T update = updates.get(i);
            writer.writeStartDocument();
            writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
            writer.writeName("q");
            getBsonDocumentCodec().encode(writer, update.getFilter(), EncoderContext.builder().build());
            writer.writeName("u");
            writeUpdate(writer, update);
            if (update.isMulti()) {
                writer.writeBoolean("multi", update.isMulti());
            }
            if (update.isUpsert()) {
                writer.writeBoolean("upsert", update.isUpsert());
            }
            writer.popMaxDocumentSize();
            writer.writeEndDocument();
            if (exceedsLimits(buffer.getPosition() - commandStartPosition, i + 1)) {
                writer.reset();
                nextMessage = createNextMessage(updates.subList(i, updates.size()));
                break;
            }
        }
        writer.writeEndArray();
        return nextMessage;
    }

    protected abstract void writeUpdate(final BsonBinaryWriter writer, final T update);

    protected abstract BaseUpdateCommandMessage<T> createNextMessage(List<T> remainingUpdates);

    @Override
    public int getItemCount() {
        return updates.size();
    }

    @Override
    protected FieldNameValidator getFieldNameValidator() {
        Map<String, FieldNameValidator> updatesMap = new HashMap<String, FieldNameValidator>();
        updatesMap.put("u", getUpdateFieldNameValidator());

        MappedFieldNameValidator updatesValidator = new MappedFieldNameValidator(new NoOpFieldNameValidator(), updatesMap);

        Map<String, FieldNameValidator> rootMap = new HashMap<String, FieldNameValidator>();
        rootMap.put("updates", updatesValidator);

        return new MappedFieldNameValidator(new NoOpFieldNameValidator(), rootMap);
    }

    protected abstract FieldNameValidator getUpdateFieldNameValidator();

    @Override
    protected String getCommandName() {
        return "update";
    }
}
