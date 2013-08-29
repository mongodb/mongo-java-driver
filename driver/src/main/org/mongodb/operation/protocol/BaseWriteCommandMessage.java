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
import org.bson.BSONBinaryWriterSettings;
import org.bson.BSONWriterSettings;
import org.bson.io.OutputBuffer;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;

import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static org.mongodb.operation.protocol.RequestMessage.OpCode.OP_QUERY;

public abstract class BaseWriteCommandMessage extends RequestMessage {
    // Server allows command document to exceed max document size by 16K, so that it can comfortably fit a stored document inside it
    private static final int HEADROOM = 16 * 1024;

    private final MongoNamespace writeNamespace;
    private final WriteConcern writeConcern;
    private final Encoder<Document> commandEncoder;

    public BaseWriteCommandMessage(final MongoNamespace writeNamespace, final WriteConcern writeConcern,
                                   final Encoder<Document> commandEncoder, final MessageSettings settings) {
        super(new MongoNamespace(writeNamespace.getDatabaseName(), COMMAND_COLLECTION_NAME).getFullName(), OP_QUERY, settings);

        this.writeNamespace = writeNamespace;
        this.writeConcern = writeConcern;
        this.commandEncoder = commandEncoder;
    }

    public MongoNamespace getWriteNamespace() {
        return writeNamespace;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public Encoder<Document> getCommandEncoder() {
        return commandEncoder;
    }

    @Override
    protected BaseWriteCommandMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        BaseWriteCommandMessage nextMessage = null;

        writeCommandHeader(buffer);

        int commandStartPosition = buffer.getPosition();
        final BSONBinaryWriter writer = new BSONBinaryWriter(new BSONWriterSettings(),
                                                             new BSONBinaryWriterSettings(getSettings().getMaxDocumentSize() + HEADROOM),
                                                             buffer, false);
        try {
            writer.writeStartDocument();
            writeCommandPrologue(writer);
            nextMessage = writeTheWrites(buffer, commandStartPosition, writer);
            writer.writeEndDocument();
        } finally {
            writer.close();
        }
        return nextMessage;
    }

    private void writeCommandHeader(final OutputBuffer buffer) {
        buffer.writeInt(0);
        buffer.writeCString(getCollectionName());

        buffer.writeInt(0);
        buffer.writeInt(-1);
    }

    protected abstract String getCommandName();

    protected abstract BaseWriteCommandMessage writeTheWrites(final OutputBuffer buffer, final int commandStartPosition,
                                                              final BSONBinaryWriter writer);

    protected boolean maximumCommandDocumentSizeExceeded(final OutputBuffer buffer, final int commandStartPosition) {
        // Subtract 2 to account for the trailing 0x0 at the end of the enclosing array and command document
        return buffer.getPosition() - commandStartPosition > getSettings().getMaxDocumentSize() + HEADROOM - 2;
    }

    private void writeCommandPrologue(final BSONBinaryWriter writer) {
        writer.writeString(getCommandName(), getWriteNamespace().getCollectionName());
        writer.writeBoolean("continueOnError", getWriteConcern().getContinueOnErrorForInsert());
        if (getWriteConcern().isAcknowledged()) {
            Document writeConcernDocument = getWriteConcern().asDocument();
            writeConcernDocument.remove("getlasterror");
            writer.writeName("writeConcern");
            getCommandEncoder().encode(writer, writeConcernDocument);
        }
    }

}
