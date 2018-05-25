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

import com.mongodb.connection.SplittablePayload;
import org.bson.BsonBinaryWriter;
import org.bson.BsonWriter;
import org.bson.io.BsonOutput;

import static com.mongodb.internal.connection.BsonWriterHelper.writePayloadArray;

class SplittablePayloadBsonWriter extends LevelCountingBsonWriter {
    private final BsonWriter writer;
    private final BsonOutput bsonOutput;
    private final SplittablePayload payload;
    private final MessageSettings settings;
    private final int messageStartPosition;

    SplittablePayloadBsonWriter(final BsonBinaryWriter writer, final BsonOutput bsonOutput, final int messageStartPosition,
                                final MessageSettings settings, final SplittablePayload payload) {
        super(writer);
        this.writer = writer;
        this.bsonOutput = bsonOutput;
        this.messageStartPosition = messageStartPosition;
        this.settings = settings;
        this.payload = payload;
    }

    @Override
    public void writeStartDocument() {
        super.writeStartDocument();
    }

    @Override
    public void writeEndDocument() {
        if (getCurrentLevel() == 0 && payload.getPayload().size() > 0) {
            writePayloadArray(writer, bsonOutput, settings, messageStartPosition, payload);
        }
        super.writeEndDocument();
    }

}
