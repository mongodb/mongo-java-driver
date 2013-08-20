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
import org.mongodb.operation.Insert;

public class InsertCommandMessage<T> extends BaseWriteCommandMessage {
    private final Insert<T> insert;
    private final Encoder<T> encoder;

    public InsertCommandMessage(final MongoNamespace namespace, final WriteConcern writeConcern, final Insert<T> insert,
                                final Encoder<Document> commandEncoder, final Encoder<T> encoder, final MessageSettings settings) {
        super(namespace, writeConcern, commandEncoder, settings);
        this.insert = insert;
        this.encoder = encoder;
    }

    protected String getCommandName() {
        return "insert";
    }

    protected InsertCommandMessage<T> writeTheWrites(final OutputBuffer buffer, final int commandStartPosition,
                                                     final BSONBinaryWriter writer) {
        InsertCommandMessage<T> nextMessage = null;
        writer.writeStartArray("documents");
        writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
        for (int i = 0; i < insert.getDocuments().size(); i++) {
            writer.mark();
            encoder.encode(writer, insert.getDocuments().get(i));
            if (maximumCommandDocumentSizeExceeded(buffer, commandStartPosition)) {
                writer.reset();
                nextMessage = new InsertCommandMessage<T>(getWriteNamespace(), getWriteConcern(), new Insert<T>(insert, i),
                        getCommandEncoder(), encoder, getSettings());
                break;
            }
        }
        writer.popMaxDocumentSize();
        writer.writeEndArray();
        return nextMessage;
    }
}
