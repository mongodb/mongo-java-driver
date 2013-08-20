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
import org.mongodb.operation.Remove;

import java.util.List;

public class DeleteCommandMessage extends BaseWriteCommandMessage {
    private final List<Remove> deletes;

    public DeleteCommandMessage(final MongoNamespace namespace, final WriteConcern writeConcern, final List<Remove> deletes,
                                final Encoder<Document> commandEncoder, final MessageSettings settings) {
        super(namespace, writeConcern, commandEncoder, settings);
        this.deletes = deletes;
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
            getCommandEncoder().encode(writer, remove.getFilter());
            writer.writeBoolean("multi", remove.isMulti());
            writer.popMaxDocumentSize();
            writer.writeEndDocument();
            if (maximumCommandDocumentSizeExceeded(buffer, commandStartPosition)) {
                writer.reset();
                nextMessage = new DeleteCommandMessage(getWriteNamespace(), getWriteConcern(), deletes.subList(i, deletes.size()),
                        getCommandEncoder(), getSettings());
                break;
            }
        }
        writer.writeEndArray();
        return nextMessage;
    }
}
