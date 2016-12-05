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

package com.mongodb.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.internal.validator.CollectibleDocumentFieldNameValidator;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.internal.validator.UpdateFieldNameValidator;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.EncoderContext;
import org.bson.io.BsonOutput;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A message for the update command.
 *
 * @mongodb.driver.manual reference/command/insert/#dbcmd.update Update Command
 */
class UpdateCommandMessage extends BaseWriteCommandMessage {
    private final List<UpdateRequest> updates;

    /**
     * Construct an instance.
     *
     * @param namespace the namespace
     * @param ordered whether the writes are ordered
     * @param writeConcern the write concern
     * @param settings the message settings
     * @param updates the list of update requests
     */
    public UpdateCommandMessage(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                final Boolean bypassDocumentValidation, final MessageSettings settings, final List<UpdateRequest> updates) {
        super(namespace, ordered, writeConcern, bypassDocumentValidation, settings);
        this.updates = updates;
    }

    /**
     * Gets the update requests.
     *
     * @return the list of update requests
     */
    public List<UpdateRequest> getRequests() {
        return Collections.unmodifiableList(updates);
    }

    @Override
    protected UpdateCommandMessage writeTheWrites(final BsonOutput bsonOutput, final int commandStartPosition,
                                                  final BsonBinaryWriter writer) {
        UpdateCommandMessage nextMessage = null;
        writer.writeStartArray("updates");
        for (int i = 0; i < updates.size(); i++) {
            writer.mark();
            UpdateRequest update = updates.get(i);
            writer.writeStartDocument();
            writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
            writer.writeName("q");
            getCodec(update.getFilter()).encode(writer, update.getFilter(), EncoderContext.builder().build());
            writer.writeName("u");

            int bufferPosition = bsonOutput.getPosition();
            getCodec(update.getUpdate()).encode(writer, update.getUpdate(), EncoderContext.builder().build());
            if (update.getType() == WriteRequest.Type.UPDATE && bsonOutput.getPosition() == bufferPosition + 8) {
                throw new IllegalArgumentException("Invalid BSON document for an update");
            }

            if (update.isMulti()) {
                writer.writeBoolean("multi", update.isMulti());
            }
            if (update.isUpsert()) {
                writer.writeBoolean("upsert", update.isUpsert());
            }
            if (update.getCollation() != null) {
                writer.writeName("collation");
                BsonDocument collation = update.getCollation().asDocument();
                getCodec(collation).encode(writer, collation, EncoderContext.builder().build());
            }
            writer.popMaxDocumentSize();
            writer.writeEndDocument();
            if (exceedsLimits(bsonOutput.getPosition() - commandStartPosition, i + 1)) {
                writer.reset();
                nextMessage = new UpdateCommandMessage(getWriteNamespace(), isOrdered(), getWriteConcern(), getBypassDocumentValidation(),
                                                       getSettings(), updates.subList(i, updates.size()));
                break;
            }
        }
        writer.writeEndArray();
        return nextMessage;
    }

    @Override
    public int getItemCount() {
        return updates.size();
    }

    @Override
    protected FieldNameValidator getFieldNameValidator() {
        Map<String, FieldNameValidator> rootMap = new HashMap<String, FieldNameValidator>();
        rootMap.put("updates", new UpdatesValidator());

        return new MappedFieldNameValidator(new NoOpFieldNameValidator(), rootMap);
    }

    @Override
    protected String getCommandName() {
        return "update";
    }

    private class UpdatesValidator implements FieldNameValidator {
        private int i = 0;

        @Override
        public boolean validate(final String fieldName) {
            return true;
        }

        @Override
        public FieldNameValidator getValidatorForField(final String fieldName) {
            if (!fieldName.equals("u")) {
                return new NoOpFieldNameValidator();
            }

            UpdateRequest updateRequest = getRequests().get(i);
            i++;

            if (updateRequest.getType() == WriteRequest.Type.REPLACE) {
                return new CollectibleDocumentFieldNameValidator();
            } else {
                return new UpdateFieldNameValidator();
            }
        }
    }
}
