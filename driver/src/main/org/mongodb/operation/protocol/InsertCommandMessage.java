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
import org.mongodb.ReadPreference;
import org.mongodb.command.Command;
import org.mongodb.operation.Insert;

public class InsertCommandMessage<T> extends BaseQueryMessage {

    // Server allows command document to exceed max document size by 16K, so that it can comfortably fit a stored document inside it
    private static final int HEADROOM = 16 * 1024;
    private final Insert<T> insert;
    private final Encoder<Document> commandEncoder;
    private final Encoder<T> encoder;
    private final MongoNamespace namespace;

    public InsertCommandMessage(final MongoNamespace namespace, final Insert<T> insert, final Encoder<Document> commandEncoder,
                                final Encoder<T> encoder, final MessageSettings settings) {
        super(new MongoNamespace(namespace.getDatabaseName(), MongoNamespace.COMMAND_COLLECTION_NAME).getFullName(), settings);
        this.namespace = namespace;
        this.insert = insert;
        this.commandEncoder = commandEncoder;
        this.encoder = encoder;
    }

    @Override
    protected InsertCommandMessage<T> encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        InsertCommandMessage<T> nextMessage = null;
        Command command = createInsertCommand();
        writeQueryPrologue(command, buffer);
        int commandStartPosition = buffer.getPosition();
        final BSONBinaryWriter writer = new BSONBinaryWriter(new BSONWriterSettings(),
                new BSONBinaryWriterSettings(getSettings().getMaxDocumentSize() + HEADROOM), buffer, false);
        try {
            writer.writeStartDocument();
            writer.writeString("insert", namespace.getCollectionName());
            writer.writeBoolean("continueOnError", insert.getWriteConcern().getContinueOnErrorForInsert());
            if (insert.getWriteConcern().callGetLastError()) {
                Document writeConcernDocument = insert.getWriteConcern().getCommand();
                writeConcernDocument.remove("getlasterror");
                writer.writeName("writeConcern");
                commandEncoder.encode(writer, writeConcernDocument);
            }
            writer.writeStartArray("documents");
            writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
            for (int i = 0; i < insert.getDocuments().size(); i++) {
                int documentStartPosition = buffer.getPosition();
                encoder.encode(writer, insert.getDocuments().get(i));
                // Subtract 2 to account for the trailing 0x00 at the end of the enclosing array and command document
                if (buffer.getPosition() - commandStartPosition > getSettings().getMaxDocumentSize() + HEADROOM - 2) {
                    buffer.truncateToPosition(documentStartPosition);
                    nextMessage = new InsertCommandMessage<T>(namespace, new Insert<T>(insert, i), commandEncoder, encoder, getSettings());
                    break;
                }
            }
            writer.popMaxDocumentSize();
            writer.writeEndArray();
            writer.writeEndDocument();
        } finally {
            writer.close();
        }
        return nextMessage;
    }

    private Command createInsertCommand() {
        return new Command(new Document("insert", namespace.getCollectionName())
                .append("writeConcern", insert.getWriteConcern().getCommand())
                .append("continueOnError", insert.getWriteConcern().getContinueOnErrorForInsert())
        ).readPreference(ReadPreference.primary());
    }
}
