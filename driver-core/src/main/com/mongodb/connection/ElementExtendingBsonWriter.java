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

import org.bson.BsonElement;
import org.bson.BsonWriter;

import java.util.List;

import static com.mongodb.connection.BsonWriterHelper.writeElements;

class ElementExtendingBsonWriter extends LevelCountingBsonWriter {
    private final List<BsonElement> extraElements;

    ElementExtendingBsonWriter(final BsonWriter writer, final List<BsonElement> extraElements) {
        super(writer);
        this.extraElements = extraElements;
    }

    @Override
    public void writeEndDocument() {
        if (getCurrentLevel() == 0) {
            writeElements(getBsonWriter(), extraElements);
        }
        super.writeEndDocument();
    }

}
