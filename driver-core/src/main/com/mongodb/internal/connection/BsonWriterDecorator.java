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
import org.bson.BsonDbPointer;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonWriter;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import static org.bson.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class BsonWriterDecorator implements BsonWriter {
    private final BsonWriter bsonWriter;

    BsonWriterDecorator(final BsonWriter bsonWriter) {
        this.bsonWriter = notNull("bsonWriter", bsonWriter);
    }

    BsonWriter getBsonWriter() {
        return bsonWriter;
    }

    @Override
    public void writeStartDocument(final String name) {
        bsonWriter.writeStartDocument(name);
    }

    @Override
    public void writeStartDocument() {
        bsonWriter.writeStartDocument();
    }

    @Override
    public void writeEndDocument() {
        bsonWriter.writeEndDocument();
    }

    @Override
    public void writeStartArray(final String name) {
        bsonWriter.writeStartArray(name);
    }

    @Override
    public void writeStartArray() {
        bsonWriter.writeStartArray();
    }

    @Override
    public void writeEndArray() {
        bsonWriter.writeEndArray();
    }

    @Override
    public void writeBinaryData(final String name, final BsonBinary binary) {
        bsonWriter.writeBinaryData(name, binary);
    }

    @Override
    public void writeBinaryData(final BsonBinary binary) {
        bsonWriter.writeBinaryData(binary);
    }

    @Override
    public void writeBoolean(final String name, final boolean value) {
        bsonWriter.writeBoolean(name, value);
    }

    @Override
    public void writeBoolean(final boolean value) {
        bsonWriter.writeBoolean(value);
    }

    @Override
    public void writeDateTime(final String name, final long value) {
        bsonWriter.writeDateTime(name, value);
    }

    @Override
    public void writeDateTime(final long value) {
        bsonWriter.writeDateTime(value);
    }

    @Override
    public void writeDBPointer(final String name, final BsonDbPointer value) {
        bsonWriter.writeDBPointer(name, value);
    }

    @Override
    public void writeDBPointer(final BsonDbPointer value) {
        bsonWriter.writeDBPointer(value);
    }

    @Override
    public void writeDouble(final String name, final double value) {
        bsonWriter.writeDouble(name, value);
    }

    @Override
    public void writeDouble(final double value) {
        bsonWriter.writeDouble(value);
    }

    @Override
    public void writeInt32(final String name, final int value) {
        bsonWriter.writeInt32(name, value);
    }

    @Override
    public void writeInt32(final int value) {
        bsonWriter.writeInt32(value);
    }

    @Override
    public void writeInt64(final String name, final long value) {
        bsonWriter.writeInt64(name, value);
    }

    @Override
    public void writeInt64(final long value) {
        bsonWriter.writeInt64(value);
    }

    @Override
    public void writeDecimal128(final Decimal128 value) {
        bsonWriter.writeDecimal128(value);
    }

    @Override
    public void writeDecimal128(final String name, final Decimal128 value) {
        bsonWriter.writeDecimal128(name, value);
    }

    @Override
    public void writeJavaScript(final String name, final String code) {
        bsonWriter.writeJavaScript(name, code);
    }

    @Override
    public void writeJavaScript(final String code) {
        bsonWriter.writeJavaScript(code);
    }

    @Override
    public void writeJavaScriptWithScope(final String name, final String code) {
        bsonWriter.writeJavaScriptWithScope(name, code);
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        bsonWriter.writeJavaScriptWithScope(code);
    }

    @Override
    public void writeMaxKey(final String name) {
        bsonWriter.writeMaxKey(name);
    }

    @Override
    public void writeMaxKey() {
        bsonWriter.writeMaxKey();
    }

    @Override
    public void writeMinKey(final String name) {
        bsonWriter.writeMinKey(name);
    }

    @Override
    public void writeMinKey() {
        bsonWriter.writeMinKey();
    }

    @Override
    public void writeName(final String name) {
        bsonWriter.writeName(name);
    }

    @Override
    public void writeNull(final String name) {
        bsonWriter.writeNull(name);
    }

    @Override
    public void writeNull() {
        bsonWriter.writeNull();
    }

    @Override
    public void writeObjectId(final String name, final ObjectId objectId) {
        bsonWriter.writeObjectId(name, objectId);
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        bsonWriter.writeObjectId(objectId);
    }

    @Override
    public void writeRegularExpression(final String name, final BsonRegularExpression regularExpression) {
        bsonWriter.writeRegularExpression(name, regularExpression);
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        bsonWriter.writeRegularExpression(regularExpression);
    }

    @Override
    public void writeString(final String name, final String value) {
        bsonWriter.writeString(name, value);
    }

    @Override
    public void writeString(final String value) {
        bsonWriter.writeString(value);
    }

    @Override
    public void writeSymbol(final String name, final String value) {
        bsonWriter.writeSymbol(name, value);
    }

    @Override
    public void writeSymbol(final String value) {
        bsonWriter.writeSymbol(value);
    }

    @Override
    public void writeTimestamp(final String name, final BsonTimestamp value) {
        bsonWriter.writeTimestamp(name, value);
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        bsonWriter.writeTimestamp(value);
    }

    @Override
    public void writeUndefined(final String name) {
        bsonWriter.writeUndefined(name);
    }

    @Override
    public void writeUndefined() {
        bsonWriter.writeUndefined();
    }

    @Override
    public void pipe(final BsonReader reader) {
        bsonWriter.pipe(reader);
    }

    @Override
    public void flush() {
        bsonWriter.flush();
    }
}
