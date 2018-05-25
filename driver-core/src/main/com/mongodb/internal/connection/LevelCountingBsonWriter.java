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

import org.bson.BsonBinary;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDbPointer;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonWriter;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import static org.bson.assertions.Assertions.notNull;


abstract class LevelCountingBsonWriter implements BsonWriter {
    private final BsonBinaryWriter bsonBinaryWriter;
    private int level = -1;

    LevelCountingBsonWriter(final BsonBinaryWriter bsonBinaryWriter) {
        this.bsonBinaryWriter = notNull("bsonBinaryWriter", bsonBinaryWriter);
    }

    public int getCurrentLevel() {
        return level;
    }

    public BsonBinaryWriter getBsonBinaryWriter() {
        return bsonBinaryWriter;
    }

    @Override
    public void writeStartDocument(final String name) {
        level++;
        bsonBinaryWriter.writeStartDocument(name);
    }

    @Override
    public void writeStartDocument() {
        level++;
        bsonBinaryWriter.writeStartDocument();
    }

    @Override
    public void writeEndDocument() {
        level--;
        bsonBinaryWriter.writeEndDocument();
    }

    @Override
    public void writeStartArray(final String name) {
        bsonBinaryWriter.writeStartArray(name);
    }

    @Override
    public void writeStartArray() {
        bsonBinaryWriter.writeStartArray();
    }

    @Override
    public void writeEndArray() {
        bsonBinaryWriter.writeEndArray();
    }

    @Override
    public void writeBinaryData(final String name, final BsonBinary binary) {
        bsonBinaryWriter.writeBinaryData(name, binary);
    }

    @Override
    public void writeBinaryData(final BsonBinary binary) {
        bsonBinaryWriter.writeBinaryData(binary);
    }

    @Override
    public void writeBoolean(final String name, final boolean value) {
        bsonBinaryWriter.writeBoolean(name, value);
    }

    @Override
    public void writeBoolean(final boolean value) {
        bsonBinaryWriter.writeBoolean(value);
    }

    @Override
    public void writeDateTime(final String name, final long value) {
        bsonBinaryWriter.writeDateTime(name, value);
    }

    @Override
    public void writeDateTime(final long value) {
        bsonBinaryWriter.writeDateTime(value);
    }

    @Override
    public void writeDBPointer(final String name, final BsonDbPointer value) {
        bsonBinaryWriter.writeDBPointer(name, value);
    }

    @Override
    public void writeDBPointer(final BsonDbPointer value) {
        bsonBinaryWriter.writeDBPointer(value);
    }

    @Override
    public void writeDouble(final String name, final double value) {
        bsonBinaryWriter.writeDouble(name, value);
    }

    @Override
    public void writeDouble(final double value) {
        bsonBinaryWriter.writeDouble(value);
    }

    @Override
    public void writeInt32(final String name, final int value) {
        bsonBinaryWriter.writeInt32(name, value);
    }

    @Override
    public void writeInt32(final int value) {
        bsonBinaryWriter.writeInt32(value);
    }

    @Override
    public void writeInt64(final String name, final long value) {
        bsonBinaryWriter.writeInt64(name, value);
    }

    @Override
    public void writeInt64(final long value) {
        bsonBinaryWriter.writeInt64(value);
    }

    @Override
    public void writeDecimal128(final Decimal128 value) {
        bsonBinaryWriter.writeDecimal128(value);
    }

    @Override
    public void writeDecimal128(final String name, final Decimal128 value) {
        bsonBinaryWriter.writeDecimal128(name, value);
    }

    @Override
    public void writeJavaScript(final String name, final String code) {
        bsonBinaryWriter.writeJavaScript(name, code);
    }

    @Override
    public void writeJavaScript(final String code) {
        bsonBinaryWriter.writeJavaScript(code);
    }

    @Override
    public void writeJavaScriptWithScope(final String name, final String code) {
        bsonBinaryWriter.writeJavaScriptWithScope(name, code);
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        bsonBinaryWriter.writeJavaScriptWithScope(code);
    }

    @Override
    public void writeMaxKey(final String name) {
        bsonBinaryWriter.writeMaxKey(name);
    }

    @Override
    public void writeMaxKey() {
        bsonBinaryWriter.writeMaxKey();
    }

    @Override
    public void writeMinKey(final String name) {
        bsonBinaryWriter.writeMinKey(name);
    }

    @Override
    public void writeMinKey() {
        bsonBinaryWriter.writeMinKey();
    }

    @Override
    public void writeName(final String name) {
        bsonBinaryWriter.writeName(name);
    }

    @Override
    public void writeNull(final String name) {
        bsonBinaryWriter.writeNull(name);
    }

    @Override
    public void writeNull() {
        bsonBinaryWriter.writeNull();
    }

    @Override
    public void writeObjectId(final String name, final ObjectId objectId) {
        bsonBinaryWriter.writeObjectId(name, objectId);
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        bsonBinaryWriter.writeObjectId(objectId);
    }

    @Override
    public void writeRegularExpression(final String name, final BsonRegularExpression regularExpression) {
        bsonBinaryWriter.writeRegularExpression(name, regularExpression);
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        bsonBinaryWriter.writeRegularExpression(regularExpression);
    }

    @Override
    public void writeString(final String name, final String value) {
        bsonBinaryWriter.writeString(name, value);
    }

    @Override
    public void writeString(final String value) {
        bsonBinaryWriter.writeString(value);
    }

    @Override
    public void writeSymbol(final String name, final String value) {
        bsonBinaryWriter.writeSymbol(name, value);
    }

    @Override
    public void writeSymbol(final String value) {
        bsonBinaryWriter.writeSymbol(value);
    }

    @Override
    public void writeTimestamp(final String name, final BsonTimestamp value) {
        bsonBinaryWriter.writeTimestamp(name, value);
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        bsonBinaryWriter.writeTimestamp(value);
    }

    @Override
    public void writeUndefined(final String name) {
        bsonBinaryWriter.writeUndefined(name);
    }

    @Override
    public void writeUndefined() {
        bsonBinaryWriter.writeUndefined();
    }

    @Override
    public void pipe(final BsonReader reader) {
        bsonBinaryWriter.pipe(reader);
    }

    @Override
    public void flush() {
        bsonBinaryWriter.flush();
    }
}
