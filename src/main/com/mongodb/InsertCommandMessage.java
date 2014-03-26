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

class InsertCommandMessage extends BaseWriteCommandMessage {
    private final List<DBObject> documents;
    private final DBEncoder encoder;

    public InsertCommandMessage(final MongoNamespace namespace, final WriteConcern writeConcern, final List<DBObject> documents,
                                final DBEncoder commandEncoder, final DBEncoder encoder, final MessageSettings settings) {
        super(namespace, writeConcern, commandEncoder, settings);
        this.documents = documents;
        this.encoder = encoder;
    }

    protected String getCommandName() {
        return "insert";
    }

    protected InsertCommandMessage writeTheWrites(final OutputBuffer buffer, final int commandStartPosition,
                                                  final BSONBinaryWriter writer) {
        InsertCommandMessage nextMessage = null;
        writer.writeStartArray("documents");
        writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
        for (int i = 0; i < documents.size(); i++) {
            writer.mark();
            writer.encodeDocument(encoder, documents.get(i));
            if (exceedsLimits(buffer.getPosition() - commandStartPosition, i + 1)) {
                writer.reset();
                nextMessage = new InsertCommandMessage(getWriteNamespace(), getWriteConcern(), documents.subList(i, documents.size()),
                                                       getCommandEncoder(), encoder, getSettings());
                break;
            }
        }
        writer.popMaxDocumentSize();
        writer.writeEndArray();
        return nextMessage;
    }

    @Override
    public int getItemCount() {
        return documents.size();
    }
}

