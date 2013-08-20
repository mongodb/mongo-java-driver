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

import org.bson.BSONBinaryWriter;
import org.bson.io.OutputBuffer;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.operation.BaseUpdate;

import java.util.List;

public abstract class BaseUpdateCommandMessage<T extends BaseUpdate> extends BaseWriteCommandMessage {
    private final List<T> updates;

    public BaseUpdateCommandMessage(final MongoNamespace writeNamespace, final WriteConcern writeConcern,
                                    final List<T> updates, final Encoder<Document> commandEncoder,
                                    final MessageSettings settings) {
        super(writeNamespace, writeConcern, commandEncoder, settings);
        this.updates = updates;
    }

    @Override
    protected BaseUpdateCommandMessage<T> writeTheWrites(final OutputBuffer buffer, final int commandStartPosition,
                                                         final BSONBinaryWriter writer) {
        BaseUpdateCommandMessage<T> nextMessage = null;
        writer.writeStartArray("updates");
        for (int i = 0; i < updates.size(); i++) {
            writer.mark();
            T update = updates.get(i);
            writer.writeStartDocument();
            writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
            writer.writeName("q");
            getCommandEncoder().encode(writer, update.getFilter());
            writer.writeName("u");
            writeUpdate(writer, update);
            writer.writeBoolean("multi", update.isMulti());
            writer.writeBoolean("upsert", update.isUpsert());
            writer.popMaxDocumentSize();
            writer.writeEndDocument();
            if (maximumCommandDocumentSizeExceeded(buffer, commandStartPosition)) {
                writer.reset();
                nextMessage = createNextMessage(updates.subList(i, updates.size()));
                break;
            }
        }
        writer.writeEndArray();
        return nextMessage;
    }

    protected abstract void writeUpdate(final BSONBinaryWriter writer, final T update);

    protected abstract BaseUpdateCommandMessage<T> createNextMessage(List<T> remainingUpdates);

    @Override
    protected String getCommandName() {
        return "update";
    }
}
