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

import org.bson.BsonWriter;


abstract class LevelCountingBsonWriter extends BsonWriterDecorator {
    static final int DEFAULT_INITIAL_LEVEL = -1;

    private int level;

    LevelCountingBsonWriter(final BsonWriter bsonWriter) {
        this(bsonWriter, DEFAULT_INITIAL_LEVEL);
    }

    /**
     * @param initialLevel This parameter allows initializing the {@linkplain #getCurrentLevel() current level}
     * with a value different from {@link #DEFAULT_INITIAL_LEVEL}.
     */
    LevelCountingBsonWriter(final BsonWriter bsonWriter, final int initialLevel) {
        super(bsonWriter);
        level = initialLevel;
    }

    int getCurrentLevel() {
        return level;
    }

    @Override
    public void writeStartDocument(final String name) {
        level++;
        super.writeStartDocument(name);
    }

    @Override
    public void writeStartDocument() {
        level++;
        super.writeStartDocument();
    }

    @Override
    public void writeEndDocument() {
        level--;
        super.writeEndDocument();
    }

    @Override
    public void writeStartArray() {
        level++;
        super.writeStartArray();
    }

    @Override
    public void writeStartArray(final String name) {
        level++;
        super.writeStartArray(name);
    }

    @Override
    public void writeEndArray() {
        level--;
        super.writeEndArray();
    }
}
