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

package com.mongodb;

import org.bson.io.OutputBuffer;

import java.util.List;

class UpdateCommandMessage extends BaseWriteCommandMessage {
    private final List<ModifyRequest> updates;
    private final DBEncoder encoder;

    public UpdateCommandMessage(final MongoNamespace writeNamespace, final WriteConcern writeConcern,
                                final List<ModifyRequest> updates, final DBEncoder commandEncoder, final DBEncoder encoder,
                                final MessageSettings settings) {
        super(writeNamespace, writeConcern, commandEncoder, settings);
        this.updates = updates;
        this.encoder = encoder;
    }

    @Override
    protected UpdateCommandMessage writeTheWrites(final OutputBuffer buffer, final int commandStartPosition,
                                                  final BSONBinaryWriter writer) {
        UpdateCommandMessage nextMessage = null;
        writer.writeStartArray("updates");
        for (int i = 0; i < updates.size(); i++) {
            writer.mark();
            ModifyRequest update = updates.get(i);
            writer.writeStartDocument();
            writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
            writer.writeName("q");
            writer.encodeDocument(getCommandEncoder(), update.getQuery());
            writer.writeName("u");
            writer.encodeDocument(encoder, update.getUpdateDocument());
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
                nextMessage = new UpdateCommandMessage(getWriteNamespace(), getWriteConcern(), updates.subList(i, updates.size()),
                                                       getCommandEncoder(), encoder, getSettings());
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
    protected String getCommandName() {
        return "update";
    }
}