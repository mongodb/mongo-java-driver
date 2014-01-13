/*
 * Copyright (c) 2008 MongoDB, Inc.
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

class DeleteCommandMessage extends BaseWriteCommandMessage {
    private final List<Remove> deletes;
    private final DBEncoder queryEncoder;

    public DeleteCommandMessage(final MongoNamespace namespace, final WriteConcern writeConcern, final List<Remove> deletes,
                                final DBEncoder commandEncoder, final DBEncoder queryEncoder, final MessageSettings settings) {
        super(namespace, writeConcern, commandEncoder, settings);
        this.deletes = deletes;
        this.queryEncoder = queryEncoder;
    }

    @Override
    protected String getCommandName() {
        return "delete";
    }

    @Override
    protected BaseWriteCommandMessage writeTheWrites(final OutputBuffer buffer, final int commandStartPosition,
                                                     final BSONBinaryWriter writer) {
        DeleteCommandMessage nextMessage = null;
        writer.writeStartArray("deletes");
        for (int i = 0; i < deletes.size(); i++) {
            writer.mark();
            Remove remove = deletes.get(i);
            writer.writeStartDocument();
            writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
            writer.writeName("q");
            writer.encodeDocument(getCommandEncoder(), remove.getFilter());
            writer.writeInt32("limit", remove.isMulti() ? 0 : 1);
            writer.popMaxDocumentSize();
            writer.writeEndDocument();
            if (maximumCommandDocumentSizeExceeded(buffer, commandStartPosition)) {
                writer.reset();
                nextMessage = new DeleteCommandMessage(getWriteNamespace(), getWriteConcern(), deletes.subList(i, deletes.size()),
                                                       getCommandEncoder(), queryEncoder, getSettings());
                break;
            }
        }
        writer.writeEndArray();
        return nextMessage;
    }
}