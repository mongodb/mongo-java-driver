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
import org.bson.BsonElement;
import org.bson.BsonReader;

import java.util.List;

import static com.mongodb.internal.connection.BsonWriterHelper.writeElements;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ElementExtendingBsonWriter extends LevelCountingBsonWriter {
    private final BsonBinaryWriter writer;
    private final List<BsonElement> extraElements;


    public ElementExtendingBsonWriter(final BsonBinaryWriter writer, final List<BsonElement> extraElements) {
        super(writer);
        this.writer = writer;
        this.extraElements = extraElements;
    }

    @Override
    public void writeEndDocument() {
        if (getCurrentLevel() == 0) {
            writeElements(writer, extraElements);
        }
        super.writeEndDocument();
    }

    @Override
    public void pipe(final BsonReader reader) {
        if (getCurrentLevel() == -1) {
            writer.pipe(reader, extraElements);
        } else {
            writer.pipe(reader);
        }
    }
}
