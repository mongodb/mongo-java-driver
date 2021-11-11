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
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.util.function.Supplier;

public class IdHoldingBsonWriter extends LevelCountingBsonWriter {

    private static final String ID_FIELD_NAME = "_id";

    private LevelCountingBsonWriter idBsonBinaryWriter;
    private BasicOutputBuffer outputBuffer;
    private String currentFieldName;
    private BsonValue id;
    private boolean idFieldIsNested = false;

    public IdHoldingBsonWriter(final BsonWriter bsonWriter) {
        super(bsonWriter);
    }

    @Override
    public void writeStartDocument(final String name) {
        setCurrentFieldName(name);

        if (isWritingId()) {
            getIdBsonWriter().writeStartDocument(name);
        }
        super.writeStartDocument(name);
    }

    @Override
    public void writeStartDocument() {
        if (isWritingId()) {
            getIdBsonWriter().writeStartDocument();
        }
        super.writeStartDocument();
    }

    @Override
    public void writeEndDocument() {
        if (isWritingId()) {
            if (getIdBsonWriter().getCurrentLevel() >= 0) {
                getIdBsonWriter().writeEndDocument();
            }

            if (getIdBsonWriter().getCurrentLevel() == -1) {
                if (id != null && id.isJavaScriptWithScope()) {
                    id = new BsonJavaScriptWithScope(id.asJavaScriptWithScope().getCode(), new RawBsonDocument(getBytes()));
                } else if (id == null) {
                    id = new RawBsonDocument(getBytes());
                }
            }
        }

        if (getCurrentLevel() == 0 && id == null) {
            id = new BsonObjectId();
            writeObjectId(ID_FIELD_NAME, id.asObjectId().getValue());
        }
        super.writeEndDocument();
    }

    @Override
    public void writeStartArray() {
        if (isWritingId()) {
            if (getIdBsonWriter().getCurrentLevel() == -1) {
                idFieldIsNested = true;
                getIdBsonWriter().writeStartDocument();
                getIdBsonWriter().writeName(ID_FIELD_NAME);
            }
            getIdBsonWriter().writeStartArray();
        }
        super.writeStartArray();
    }

    @Override
    public void writeStartArray(final String name) {
        setCurrentFieldName(name);
        if (isWritingId()) {
            if (getIdBsonWriter().getCurrentLevel() == -1) {
                getIdBsonWriter().writeStartDocument();
            }
            getIdBsonWriter().writeStartArray(name);
        }
        super.writeStartArray(name);
    }

    @Override
    public void writeEndArray() {
        if (isWritingId()) {
            getIdBsonWriter().writeEndArray();
            if (getIdBsonWriter().getCurrentLevel() == 0 && idFieldIsNested) {
                getIdBsonWriter().writeEndDocument();
                id = new RawBsonDocument(getBytes()).get(ID_FIELD_NAME);
            }
        }
        super.writeEndArray();
    }

    @Override
    public void writeBinaryData(final String name, final BsonBinary binary) {
        setCurrentFieldName(name);
        addBsonValue(() -> binary, () -> getIdBsonWriter().writeBinaryData(name, binary));
        super.writeBinaryData(name, binary);
    }

    @Override
    public void writeBinaryData(final BsonBinary binary) {
        addBsonValue(() -> binary, () -> getIdBsonWriter().writeBinaryData(binary));
        super.writeBinaryData(binary);
    }

    @Override
    public void writeBoolean(final String name, final boolean value) {
        setCurrentFieldName(name);
        addBsonValue(() -> BsonBoolean.valueOf(value), () -> getIdBsonWriter().writeBoolean(name, value));
        super.writeBoolean(name, value);
    }

    @Override
    public void writeBoolean(final boolean value) {
        addBsonValue(() -> BsonBoolean.valueOf(value), () -> getIdBsonWriter().writeBoolean(value));
        super.writeBoolean(value);
    }

    @Override
    public void writeDateTime(final String name, final long value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonDateTime(value), () -> getIdBsonWriter().writeDateTime(name, value));
        super.writeDateTime(name, value);
    }

    @Override
    public void writeDateTime(final long value) {
        addBsonValue(() -> new BsonDateTime(value), () -> getIdBsonWriter().writeDateTime(value));
        super.writeDateTime(value);
    }

    @Override
    public void writeDBPointer(final String name, final BsonDbPointer value) {
        setCurrentFieldName(name);
        addBsonValue(() -> value, () -> getIdBsonWriter().writeDBPointer(name, value));
        super.writeDBPointer(name, value);
    }

    @Override
    public void writeDBPointer(final BsonDbPointer value) {
        addBsonValue(() -> value, () -> getIdBsonWriter().writeDBPointer(value));
        super.writeDBPointer(value);
    }

    @Override
    public void writeDouble(final String name, final double value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonDouble(value), () -> getIdBsonWriter().writeDouble(name, value));
        super.writeDouble(name, value);
    }

    @Override
    public void writeDouble(final double value) {
        addBsonValue(() -> new BsonDouble(value), () -> getIdBsonWriter().writeDouble(value));
        super.writeDouble(value);
    }

    @Override
    public void writeInt32(final String name, final int value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonInt32(value), () -> getIdBsonWriter().writeInt32(name, value));
        super.writeInt32(name, value);
    }

    @Override
    public void writeInt32(final int value) {
        addBsonValue(() -> new BsonInt32(value), () -> getIdBsonWriter().writeInt32(value));
        super.writeInt32(value);
    }

    @Override
    public void writeInt64(final String name, final long value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonInt64(value), () -> getIdBsonWriter().writeInt64(name, value));
        super.writeInt64(name, value);
    }

    @Override
    public void writeInt64(final long value) {
        addBsonValue(() -> new BsonInt64(value), () -> getIdBsonWriter().writeInt64(value));
        super.writeInt64(value);
    }

    @Override
    public void writeDecimal128(final String name, final Decimal128 value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonDecimal128(value), () -> getIdBsonWriter().writeDecimal128(name, value));
        super.writeDecimal128(name, value);
    }

    @Override
    public void writeDecimal128(final Decimal128 value) {
        addBsonValue(() -> new BsonDecimal128(value), () -> getIdBsonWriter().writeDecimal128(value));
        super.writeDecimal128(value);
    }

    @Override
    public void writeJavaScript(final String name, final String code) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonJavaScript(code), () -> getIdBsonWriter().writeJavaScript(name, code));
        super.writeJavaScript(name, code);
    }

    @Override
    public void writeJavaScript(final String code) {
        addBsonValue(() -> new BsonJavaScript(code), () -> getIdBsonWriter().writeJavaScript(code));
        super.writeJavaScript(code);
    }

    @Override
    public void writeJavaScriptWithScope(final String name, final String code) {
        addBsonValue(() -> new BsonJavaScriptWithScope(code, new BsonDocument()),
                () -> getIdBsonWriter().writeJavaScriptWithScope(name, code));
        super.writeJavaScriptWithScope(name, code);
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        addBsonValue(() -> new BsonJavaScriptWithScope(code, new BsonDocument()), () -> getIdBsonWriter().writeJavaScriptWithScope(code));
        super.writeJavaScriptWithScope(code);
    }

    @Override
    public void writeMaxKey(final String name) {
        setCurrentFieldName(name);
        addBsonValue(BsonMaxKey::new, () -> getIdBsonWriter().writeMaxKey(name));
        super.writeMaxKey(name);
    }

    @Override
    public void writeMaxKey() {
        addBsonValue(BsonMaxKey::new, idBsonBinaryWriter::writeMaxKey);
        super.writeMaxKey();
    }

    @Override
    public void writeMinKey(final String name) {
        setCurrentFieldName(name);
        addBsonValue(BsonMinKey::new, () -> getIdBsonWriter().writeMinKey(name));
        super.writeMinKey(name);
    }

    @Override
    public void writeMinKey() {
        addBsonValue(BsonMinKey::new, idBsonBinaryWriter::writeMinKey);
        super.writeMinKey();
    }

    @Override
    public void writeName(final String name) {
        setCurrentFieldName(name);
        if (getIdBsonWriter().getCurrentLevel() >= 0) {
            getIdBsonWriter().writeName(name);
        }
        super.writeName(name);
    }

    @Override
    public void writeNull(final String name) {
        setCurrentFieldName(name);
        addBsonValue(BsonNull::new, () -> getIdBsonWriter().writeNull(name));
        super.writeNull(name);
    }

    @Override
    public void writeNull() {
        addBsonValue(BsonNull::new, idBsonBinaryWriter::writeNull);
        super.writeNull();
    }

    @Override
    public void writeObjectId(final String name, final ObjectId objectId) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonObjectId(objectId), () -> getIdBsonWriter().writeObjectId(name, objectId));
        super.writeObjectId(name, objectId);
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        addBsonValue(() -> new BsonObjectId(objectId), () -> getIdBsonWriter().writeObjectId(objectId));
        super.writeObjectId(objectId);
    }

    @Override
    public void writeRegularExpression(final String name, final BsonRegularExpression regularExpression) {
        setCurrentFieldName(name);
        addBsonValue(() -> regularExpression, () -> getIdBsonWriter().writeRegularExpression(name, regularExpression));
        super.writeRegularExpression(name, regularExpression);
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        addBsonValue(() -> regularExpression, () -> getIdBsonWriter().writeRegularExpression(regularExpression));
        super.writeRegularExpression(regularExpression);
    }

    @Override
    public void writeString(final String name, final String value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonString(value), () -> getIdBsonWriter().writeString(name, value));
        super.writeString(name, value);
    }

    @Override
    public void writeString(final String value) {
        addBsonValue(() -> new BsonString(value), () -> getIdBsonWriter().writeString(value));
        super.writeString(value);
    }

    @Override
    public void writeSymbol(final String name, final String value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonSymbol(value), () -> getIdBsonWriter().writeSymbol(name, value));
        super.writeSymbol(name, value);
    }

    @Override
    public void writeSymbol(final String value) {
        addBsonValue(() -> new BsonSymbol(value), () -> getIdBsonWriter().writeSymbol(value));
        super.writeSymbol(value);
    }

    @Override
    public void writeTimestamp(final String name, final BsonTimestamp value) {
        setCurrentFieldName(name);
        addBsonValue(() -> value, () -> getIdBsonWriter().writeTimestamp(name, value));
        super.writeTimestamp(name, value);
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        addBsonValue(() -> value, () -> getIdBsonWriter().writeTimestamp(value));
        super.writeTimestamp(value);
    }

    @Override
    public void writeUndefined(final String name) {
        setCurrentFieldName(name);
        addBsonValue(BsonUndefined::new, () -> getIdBsonWriter().writeUndefined(name));
        super.writeUndefined(name);
    }

    @Override
    public void writeUndefined() {
        addBsonValue(BsonUndefined::new, idBsonBinaryWriter::writeUndefined);
        super.writeUndefined();
    }

    @Override
    public void pipe(final BsonReader reader) {
        super.pipe(reader);
    }

    @Override
    public void flush() {
        super.flush();
    }

    public BsonValue getId() {
        return id;
    }

    private void setCurrentFieldName(final String name) {
        currentFieldName = name;
    }

    private boolean isWritingId() {
        return getIdBsonWriter().getCurrentLevel() >= 0 || (getCurrentLevel() == 0 && currentFieldName != null
                && currentFieldName.equals(ID_FIELD_NAME));
    }

    private void addBsonValue(final Supplier<BsonValue> value, final Runnable writeValue) {
        if (isWritingId()) {
            if (getIdBsonWriter().getCurrentLevel() >= 0) {
                writeValue.run();
            } else {
                id = value.get();
            }
        }
    }

    private LevelCountingBsonWriter getIdBsonWriter() {
        if (idBsonBinaryWriter == null) {
            outputBuffer = new BasicOutputBuffer();
            idBsonBinaryWriter = new LevelCountingBsonWriter(new BsonBinaryWriter(outputBuffer)){};
        }
        return idBsonBinaryWriter;
    }

    private byte[] getBytes() {
        return outputBuffer.getInternalBuffer();
    }

}
