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

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;

abstract class BaseWriteCommandMessage extends RequestMessage {
    // Server allows command document to exceed max document size by 16K, so that it can comfortably fit a stored document inside it
    private static final int HEADROOM = 16 * 1024;

    private final MongoNamespace writeNamespace;
    private final WriteConcern writeConcern;
    private final DBEncoder commandEncoder;

    public BaseWriteCommandMessage(final MongoNamespace writeNamespace, final WriteConcern writeConcern,
                                   final DBEncoder commandEncoder, final MessageSettings settings) {
        super(new MongoNamespace(writeNamespace.getDatabaseName(), COMMAND_COLLECTION_NAME).getFullName(), OpCode.OP_QUERY, settings);

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

    public DBEncoder getCommandEncoder() {
        return commandEncoder;
    }

    public BaseWriteCommandMessage encode(final OutputBuffer buffer) {
        return (BaseWriteCommandMessage) super.encode(buffer);
    }

    @Override
    protected BaseWriteCommandMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        BaseWriteCommandMessage nextMessage = null;

        writeCommandHeader(buffer);

        int commandStartPosition = buffer.getPosition();
        final BSONBinaryWriter writer = new BSONBinaryWriter(new BSONWriterSettings(),
                                                             new BSONBinaryWriterSettings(getSettings().getMaxDocumentSize() + HEADROOM),
                                                             buffer);
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

    protected boolean exceedsLimits(final int batchLength, final int batchItemCount) {
        return (exceedsBatchLengthLimit(batchLength, batchItemCount) || exceedsBatchItemCountLimit(batchItemCount));
    }

    // make a special exception for a command with only a single item added to it.  It's allowed to exceed maximum document size so that
    // it's possible to, say, send a replacement document that is itself 16MB, which would push the size of the containing command
    // document to be greater than the maximum document size.
    private boolean exceedsBatchLengthLimit(final int batchLength, final int batchItemCount) {
        return batchLength > getSettings().getMaxDocumentSize() && batchItemCount > 1;
    }

    private boolean exceedsBatchItemCountLimit(final int batchItemCount) {
        return batchItemCount > getSettings().getMaxWriteBatchSize();
    }

    public abstract int getItemCount();

    private void writeCommandPrologue(final BSONBinaryWriter writer) {
        writer.writeString(getCommandName(), getWriteNamespace().getCollectionName());
        writer.writeBoolean("ordered", !getWriteConcern().getContinueOnError());
        if (!getWriteConcern().useServerDefault()) {
            writer.writeName("writeConcern");
            writer.encodeDocument(getCommandEncoder(), getWriteConcern().asDBObject());
        }
    }
}
