/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import org.bson.BsonBinaryWriter;
import org.bson.BsonWriter;
import org.bson.io.BsonOutput;

import static com.mongodb.internal.connection.BsonWriterHelper.writePayloadArray;

public class SplittablePayloadBsonWriter extends LevelCountingBsonWriter {
    private final BsonWriter writer;
    private final BsonOutput bsonOutput;
    private final SplittablePayload payload;
    private int maxSplittableDocumentSize;
    private final MessageSettings settings;
    private final int messageStartPosition;

    public SplittablePayloadBsonWriter(final BsonBinaryWriter writer, final BsonOutput bsonOutput,
                                       final MessageSettings settings, final SplittablePayload payload,
                                       final int maxSplittableDocumentSize) {
        this(writer, bsonOutput, 0, settings, payload, maxSplittableDocumentSize);
    }

    public SplittablePayloadBsonWriter(final BsonBinaryWriter writer, final BsonOutput bsonOutput, final int messageStartPosition,
                                       final MessageSettings settings, final SplittablePayload payload) {
        this(writer, bsonOutput, messageStartPosition, settings, payload, settings.getMaxDocumentSize());
    }

    public SplittablePayloadBsonWriter(final BsonBinaryWriter writer, final BsonOutput bsonOutput, final int messageStartPosition,
                                       final MessageSettings settings, final SplittablePayload payload,
                                       final int maxSplittableDocumentSize) {
        super(writer);
        this.writer = writer;
        this.bsonOutput = bsonOutput;
        this.messageStartPosition = messageStartPosition;
        this.settings = settings;
        this.payload = payload;
        this.maxSplittableDocumentSize = maxSplittableDocumentSize;
    }

    @Override
    public void writeStartDocument() {
        super.writeStartDocument();
    }

    @Override
    public void writeEndDocument() {
        if (getCurrentLevel() == 0 && payload.hasPayload()) {
            writePayloadArray(writer, bsonOutput, settings, messageStartPosition, payload, maxSplittableDocumentSize);
        }
        super.writeEndDocument();
    }

}
