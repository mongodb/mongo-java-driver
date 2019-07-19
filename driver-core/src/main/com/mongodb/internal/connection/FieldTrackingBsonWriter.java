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

// Helper class to help determine when an update document contains any fields
// It's an imperfect check because we can't tell if the pipe method ended up writing any fields.
// For the purposes of the check, it's better to assume that pipe does end up writing a field, in order to avoid
// incorrectly reporting an error any time pipe is used
public class FieldTrackingBsonWriter extends BsonWriterDecorator {

    private boolean hasWrittenField;
    private boolean topLevelDocumentWritten;

    public FieldTrackingBsonWriter(final BsonWriter bsonWriter) {
        super(bsonWriter);
    }

    public boolean hasWrittenField() {
        return hasWrittenField;
    }

    @Override
    public void writeStartDocument(final String name) {
        if (topLevelDocumentWritten) {
            hasWrittenField = true;
        }
        super.writeStartDocument(name);
    }

    @Override
    public void writeStartDocument() {
        if (topLevelDocumentWritten) {
            hasWrittenField = true;
        }
        topLevelDocumentWritten = true;
        super.writeStartDocument();
    }

    @Override
    public void writeStartArray(final String name) {
        hasWrittenField = true;
        super.writeStartArray(name);
    }

    @Override
    public void writeStartArray() {
        hasWrittenField = true;
        super.writeStartArray();
    }

    @Override
    public void writeBinaryData(final String name, final BsonBinary binary) {
        hasWrittenField = true;
        super.writeBinaryData(name, binary);
    }

    @Override
    public void writeBinaryData(final BsonBinary binary) {
        hasWrittenField = true;
        super.writeBinaryData(binary);
    }

    @Override
    public void writeBoolean(final String name, final boolean value) {
        hasWrittenField = true;
        super.writeBoolean(name, value);
    }

    @Override
    public void writeBoolean(final boolean value) {
        hasWrittenField = true;
        super.writeBoolean(value);
    }

    @Override
    public void writeDateTime(final String name, final long value) {
        hasWrittenField = true;
        super.writeDateTime(name, value);
    }

    @Override
    public void writeDateTime(final long value) {
        hasWrittenField = true;
        super.writeDateTime(value);
    }

    @Override
    public void writeDBPointer(final String name, final BsonDbPointer value) {
        hasWrittenField = true;
        super.writeDBPointer(name, value);
    }

    @Override
    public void writeDBPointer(final BsonDbPointer value) {
        hasWrittenField = true;
        super.writeDBPointer(value);
    }

    @Override
    public void writeDouble(final String name, final double value) {
        hasWrittenField = true;
        super.writeDouble(name, value);
    }

    @Override
    public void writeDouble(final double value) {
        hasWrittenField = true;
        super.writeDouble(value);
    }

    @Override
    public void writeInt32(final String name, final int value) {
        hasWrittenField = true;
        super.writeInt32(name, value);
    }

    @Override
    public void writeInt32(final int value) {
        hasWrittenField = true;
        super.writeInt32(value);
    }

    @Override
    public void writeInt64(final String name, final long value) {
        super.writeInt64(name, value);
        hasWrittenField = true;
    }

    @Override
    public void writeInt64(final long value) {
        hasWrittenField = true;
        super.writeInt64(value);
    }

    @Override
    public void writeDecimal128(final Decimal128 value) {
        hasWrittenField = true;
        super.writeDecimal128(value);
    }

    @Override
    public void writeDecimal128(final String name, final Decimal128 value) {
        hasWrittenField = true;
        super.writeDecimal128(name, value);
    }

    @Override
    public void writeJavaScript(final String name, final String code) {
        hasWrittenField = true;
        super.writeJavaScript(name, code);
    }

    @Override
    public void writeJavaScript(final String code) {
        hasWrittenField = true;
        super.writeJavaScript(code);
    }

    @Override
    public void writeJavaScriptWithScope(final String name, final String code) {
        super.writeJavaScriptWithScope(name, code);
        hasWrittenField = true;
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        hasWrittenField = true;
        super.writeJavaScriptWithScope(code);
    }

    @Override
    public void writeMaxKey(final String name) {
        hasWrittenField = true;
        super.writeMaxKey(name);
    }

    @Override
    public void writeMaxKey() {
        hasWrittenField = true;
        super.writeMaxKey();
    }

    @Override
    public void writeMinKey(final String name) {
        hasWrittenField = true;
        super.writeMinKey(name);
    }

    @Override
    public void writeMinKey() {
        hasWrittenField = true;
        super.writeMinKey();
    }

    @Override
    public void writeNull(final String name) {
        hasWrittenField = true;
        super.writeNull(name);
    }

    @Override
    public void writeNull() {
        hasWrittenField = true;
        super.writeNull();
    }

    @Override
    public void writeObjectId(final String name, final ObjectId objectId) {
        hasWrittenField = true;
        super.writeObjectId(name, objectId);
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        hasWrittenField = true;
        super.writeObjectId(objectId);
    }

    @Override
    public void writeRegularExpression(final String name, final BsonRegularExpression regularExpression) {
        hasWrittenField = true;
        super.writeRegularExpression(name, regularExpression);
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        hasWrittenField = true;
        super.writeRegularExpression(regularExpression);
    }

    @Override
    public void writeString(final String name, final String value) {
        hasWrittenField = true;
        super.writeString(name, value);
    }

    @Override
    public void writeString(final String value) {
        hasWrittenField = true;
        super.writeString(value);
    }

    @Override
    public void writeSymbol(final String name, final String value) {
        hasWrittenField = true;
        super.writeSymbol(name, value);
    }

    @Override
    public void writeSymbol(final String value) {
        hasWrittenField = true;
        super.writeSymbol(value);
    }

    @Override
    public void writeTimestamp(final String name, final BsonTimestamp value) {
        hasWrittenField = true;
        super.writeTimestamp(name, value);
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        hasWrittenField = true;
        super.writeTimestamp(value);
    }

    @Override
    public void writeUndefined(final String name) {
        hasWrittenField = true;
        super.writeUndefined(name);
    }

    @Override
    public void writeUndefined() {
        hasWrittenField = true;
        super.writeUndefined();
    }

    @Override
    public void pipe(final BsonReader reader) {
        // this is a faulty assumption, as we may end up piping an empty document.  But if we don't increment here, we may undercount,
        // which in this context is worse since we'll thrown an exception when we should not.
        hasWrittenField = true;
        super.pipe(reader);
    }
}
