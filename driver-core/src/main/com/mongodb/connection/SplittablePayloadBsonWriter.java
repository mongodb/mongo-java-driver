/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection;

import org.bson.BsonWriter;
import org.bson.io.BsonOutput;

import static com.mongodb.connection.BsonWriterHelper.writePayloadArray;


class SplittablePayloadBsonWriter extends LevelCountingBsonWriter {
    private final BsonWriter writer;
    private final BsonOutput bsonOutput;
    private final SplittablePayload payload;
    private final MessageSettings settings;
    private int commandStartPosition;

    SplittablePayloadBsonWriter(final BsonWriter writer, final BsonOutput bsonOutput, final MessageSettings settings,
                                final SplittablePayload payload) {
        super(writer);
        this.writer = writer;
        this.bsonOutput = bsonOutput;
        this.settings = settings;
        this.payload = payload;
    }

    @Override
    public void writeStartDocument() {
        super.writeStartDocument();
        if (getCurrentLevel() == 0) {
            commandStartPosition = bsonOutput.getPosition();
        }
    }

    @Override
    public void writeEndDocument() {
        if (getCurrentLevel() == 0 && payload.getPayload().size() > 0) {
            writePayloadArray(writer, bsonOutput, settings, commandStartPosition, payload);
        }
        super.writeEndDocument();
    }

}
