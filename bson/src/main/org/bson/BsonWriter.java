/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

package org.bson;

import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * An interface for writing a logical BSON document using a push-oriented API.
 *
 * @since 3.0
 */
public interface BsonWriter {
    /**
     * Flushes any pending data to the output destination.
     */
    void flush();

    /**
     * Writes a BSON Binary data element to the writer.
     *
     * @param binary The Binary data.
     */
    void writeBinaryData(BsonBinary binary);

    /**
     * Writes a BSON Binary data element to the writer.
     *
     * @param name   The name of the element.
     * @param binary The Binary data value.
     */
    void writeBinaryData(String name, BsonBinary binary);

    /**
     * Writes a BSON Boolean to the writer.
     *
     * @param value The Boolean value.
     */
    void writeBoolean(boolean value);

    /**
     * Writes a BSON Boolean element to the writer.
     *
     * @param name  The name of the element.
     * @param value The Boolean value.
     */
    void writeBoolean(String name, boolean value);

    /**
     * Writes a BSON DateTime to the writer.
     *
     * @param value The number of milliseconds since the Unix epoch.
     */
    void writeDateTime(long value);

    /**
     * Writes a BSON DateTime element to the writer.
     *
     * @param name  The name of the element.
     * @param value The number of milliseconds since the Unix epoch.
     */
    void writeDateTime(String name, long value);

    /**
     * Writes a BSON DBPointer to the writer.
     *
     * @param value The DBPointer to write
     */
    void writeDBPointer(BsonDbPointer value);

    /**
     * Writes a BSON DBPointer element to the writer.
     *
     * @param name  The name of the element.
     * @param value The DBPointer to write
     */
    void writeDBPointer(String name, BsonDbPointer value);

    /**
     * Writes a BSON Double to the writer.
     *
     * @param value The Double value.
     */
    void writeDouble(double value);

    /**
     * Writes a BSON Double element to the writer.
     *
     * @param name  The name of the element.
     * @param value The Double value.
     */
    void writeDouble(String name, double value);

    /**
     * Writes the end of a BSON array to the writer.
     */
    void writeEndArray();

    /**
     * Writes the end of a BSON document to the writer.
     */
    void writeEndDocument();

    /**
     * Writes a BSON Int32 to the writer.
     *
     * @param value The Int32 value.
     */
    void writeInt32(int value);

    /**
     * Writes a BSON Int32 element to the writer.
     *
     * @param name  The name of the element.
     * @param value The Int32 value.
     */
    void writeInt32(String name, int value);

    /**
     * Writes a BSON Int64 to the writer.
     *
     * @param value The Int64 value.
     */
    void writeInt64(long value);

    /**
     * Writes a BSON Int64 element to the writer.
     *
     * @param name  The name of the element.
     * @param value The Int64 value.
     */
    void writeInt64(String name, long value);

    /**
     * Writes a BSON Decimal128 to the writer.
     *
     * @param value The Decimal128 value.
     * @since 3.4
     */
    void writeDecimal128(Decimal128 value);

    /**
     * Writes a BSON Decimal128 element to the writer.
     *
     * @param name  The name of the element.
     * @param value The Decimal128 value.
     * @since 3.4
     */
    void writeDecimal128(String name, Decimal128 value);

    /**
     * Writes a BSON JavaScript to the writer.
     *
     * @param code The JavaScript code.
     */
    void writeJavaScript(String code);

    /**
     * Writes a BSON JavaScript element to the writer.
     *
     * @param name The name of the element.
     * @param code The JavaScript code.
     */
    void writeJavaScript(String name, String code);

    /**
     * Writes a BSON JavaScript to the writer (call WriteStartDocument to start writing the scope).
     *
     * @param code The JavaScript code.
     */
    void writeJavaScriptWithScope(String code);

    /**
     * Writes a BSON JavaScript element to the writer (call WriteStartDocument to start writing the scope).
     *
     * @param name The name of the element.
     * @param code The JavaScript code.
     */
    void writeJavaScriptWithScope(String name, String code);

    /**
     * Writes a BSON MaxKey to the writer.
     */
    void writeMaxKey();

    /**
     * Writes a BSON MaxKey element to the writer.
     *
     * @param name The name of the element.
     */
    void writeMaxKey(String name);

    /**
     * Writes a BSON MinKey to the writer.
     */
    void writeMinKey();

    /**
     * Writes a BSON MinKey element to the writer.
     *
     * @param name The name of the element.
     */
    void writeMinKey(String name);

    /**
     * Writes the name of an element to the writer.
     *
     * @param name The name of the element.
     */
    void writeName(String name);

    /**
     * Writes a BSON null to the writer.
     */
    void writeNull();

    /**
     * Writes a BSON null element to the writer.
     *
     * @param name The name of the element.
     */
    void writeNull(String name);

    /**
     * Writes a BSON ObjectId to the writer.
     *
     * @param objectId The ObjectId value.
     */
    void writeObjectId(ObjectId objectId);

    /**
     * Writes a BSON ObjectId element to the writer.
     *
     * @param name     The name of the element.
     * @param objectId The ObjectId value.
     */
    void writeObjectId(String name, ObjectId objectId);

    /**
     * Writes a BSON regular expression to the writer.
     *
     * @param regularExpression the regular expression to write.
     */
    void writeRegularExpression(BsonRegularExpression regularExpression);

    /**
     * Writes a BSON regular expression element to the writer.
     *
     * @param name              The name of the element.
     * @param regularExpression The RegularExpression value.
     */
    void writeRegularExpression(String name, BsonRegularExpression regularExpression);

    /**
     * Writes the start of a BSON array to the writer.
     *
     * @throws BsonSerializationException if maximum serialization depth exceeded.
     */
    void writeStartArray();

    /**
     * Writes the start of a BSON array element to the writer.
     *
     * @param name The name of the element.
     */
    void writeStartArray(String name);

    /**
     * Writes the start of a BSON document to the writer.
     *
     * @throws BsonSerializationException if maximum serialization depth exceeded.
     */
    void writeStartDocument();

    /**
     * Writes the start of a BSON document element to the writer.
     *
     * @param name The name of the element.
     */
    void writeStartDocument(String name);

    /**
     * Writes a BSON String to the writer.
     *
     * @param value The String value.
     */
    void writeString(String value);

    /**
     * Writes a BSON String element to the writer.
     *
     * @param name  The name of the element.
     * @param value The String value.
     */
    void writeString(String name, String value);

    /**
     * Writes a BSON Symbol to the writer.
     *
     * @param value The symbol.
     */
    void writeSymbol(String value);

    /**
     * Writes a BSON Symbol element to the writer.
     *
     * @param name  The name of the element.
     * @param value The symbol.
     */
    void writeSymbol(String name, String value);

    /**
     * Writes a BSON Timestamp to the writer.
     *
     * @param value The combined timestamp/increment value.
     */
    void writeTimestamp(BsonTimestamp value);

    /**
     * Writes a BSON Timestamp element to the writer.
     *
     * @param name  The name of the element.
     * @param value The combined timestamp/increment value.
     */
    void writeTimestamp(String name, BsonTimestamp value);

    /**
     * Writes a BSON undefined to the writer.
     */
    void writeUndefined();

    /**
     * Writes a BSON undefined element to the writer.
     *
     * @param name The name of the element.
     */
    void writeUndefined(String name);

    /**
     * Reads a single document from a BsonReader and writes it to this.
     *
     * @param reader The source.
     */
    void pipe(BsonReader reader);

    /**
     * Reads a single document from a BsonReader and writes it to this, appending the given extra elements to the end of
     * the document.
     *
     * @param reader The source.
     * @param extraElements The extra BSON elements to append
     * @since 3.6
     */
    void pipe(BsonReader reader, List<BsonElement> extraElements);
}
