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

package org.bson;

import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.io.Closeable;

/**
 * An interface for reading a logical BSON document using a pull-oriented API.
 *
 * @since 3.0
 */
public interface BsonReader extends Closeable {
    /**
     * @return The current BsonType.
     */
    BsonType getCurrentBsonType();

    /**
     * Gets the most recently read name.
     *
     * @return the most recently read name
     */
    String getCurrentName();

    /**
     * Reads BSON Binary data from the reader.
     *
     * @return A Binary.
     */
    BsonBinary readBinaryData();

    /**
     * Peeks the subtype of the binary data that the reader is positioned at.  This operation is not permitted if the mark is already set.
     *
     * @return the subtype
     * @see #getMark()
     */
    byte peekBinarySubType();

    /**
     * Peeks the size of the binary data that the reader is positioned at.  This operation is not permitted if the mark is already set.
     *
     * @return the size of the binary data
     * @see #getMark()
     * @since 3.4
     */
    int peekBinarySize();

    /**
     * Reads a BSON Binary data element from the reader.
     *
     * @param name The name of the element.
     * @return A Binary.
     */
    BsonBinary readBinaryData(String name);

    /**
     * Reads a BSON Boolean from the reader.
     *
     * @return A Boolean.
     */
    boolean readBoolean();

    /**
     * Reads a BSON Boolean element from the reader.
     *
     * @param name The name of the element.
     * @return A Boolean.
     */
    boolean readBoolean(String name);

    /**
     * Reads a BSONType from the reader.
     *
     * @return A BSONType.
     */
    BsonType readBsonType();

    /**
     * Reads a BSON DateTime from the reader.
     *
     * @return The number of milliseconds since the Unix epoch.
     */
    long readDateTime();

    /**
     * Reads a BSON DateTime element from the reader.
     *
     * @param name The name of the element.
     * @return The number of milliseconds since the Unix epoch.
     */
    long readDateTime(String name);

    /**
     * Reads a BSON Double from the reader.
     *
     * @return A Double.
     */
    double readDouble();

    /**
     * Reads a BSON Double element from the reader.
     *
     * @param name The name of the element.
     * @return A Double.
     */
    double readDouble(String name);

    /**
     * Reads the end of a BSON array from the reader.
     */
    void readEndArray();

    /**
     * Reads the end of a BSON document from the reader.
     */
    void readEndDocument();

    /**
     * Reads a BSON Int32 from the reader.
     *
     * @return An Int32.
     */
    int readInt32();

    /**
     * Reads a BSON Int32 element from the reader.
     *
     * @param name The name of the element.
     * @return An Int32.
     */
    int readInt32(String name);

    /**
     * Reads a BSON Int64 from the reader.
     *
     * @return An Int64.
     */
    long readInt64();

    /**
     * Reads a BSON Int64 element from the reader.
     *
     * @param name The name of the element.
     * @return An Int64.
     */
    long readInt64(String name);

    /**
     * Reads a BSON Decimal128 from the reader.
     *
     * @return A Decimal128
     * @since 3.4
     */
    Decimal128 readDecimal128();

    /**
     * Reads a BSON Decimal128 element from the reader.
     *
     * @param name The name of the element.
     * @return A Decimal128
     * @since 3.4
     */
    Decimal128 readDecimal128(String name);

    /**
     * Reads a BSON JavaScript from the reader.
     *
     * @return A string.
     */
    String readJavaScript();

    /**
     * Reads a BSON JavaScript element from the reader.
     *
     * @param name The name of the element.
     * @return A string.
     */
    String readJavaScript(String name);

    /**
     * Reads a BSON JavaScript with scope from the reader (call readStartDocument next to read the scope).
     *
     * @return A string.
     */
    String readJavaScriptWithScope();

    /**
     * Reads a BSON JavaScript with scope element from the reader (call readStartDocument next to read the scope).
     *
     * @param name The name of the element.
     * @return A string.
     */
    String readJavaScriptWithScope(String name);

    /**
     * Reads a BSON MaxKey from the reader.
     */
    void readMaxKey();

    /**
     * Reads a BSON MaxKey element from the reader.
     *
     * @param name The name of the element.
     */
    void readMaxKey(String name);

    /**
     * Reads a BSON MinKey from the reader.
     */
    void readMinKey();

    /**
     * Reads a BSON MinKey element from the reader.
     *
     * @param name The name of the element.
     */
    void readMinKey(String name);

    /**
     * Reads the name of an element from the reader.
     *
     * @return The name of the element.
     */
    String readName();

    /**
     * Reads the name of an element from the reader.
     *
     * @param name The name of the element.
     */
    void readName(String name);

    /**
     * Reads a BSON null from the reader.
     */
    void readNull();

    /**
     * Reads a BSON null element from the reader.
     *
     * @param name The name of the element.
     */
    void readNull(String name);

    /**
     * Reads a BSON ObjectId from the reader.
     *
     * @return the {@code ObjectId} value
     */
    ObjectId readObjectId();

    /**
     * Reads a BSON ObjectId element from the reader.
     *
     * @param name The name of the element.
     * @return ObjectId.
     */
    ObjectId readObjectId(String name);

    /**
     * Reads a BSON regular expression from the reader.
     *
     * @return A regular expression.
     */
    BsonRegularExpression readRegularExpression();

    /**
     * Reads a BSON regular expression element from the reader.
     *
     * @param name The name of the element.
     * @return A regular expression.
     */
    BsonRegularExpression readRegularExpression(String name);

    /**
     * Reads a BSON DBPointer from the reader.
     *
     * @return A DBPointer.
     */
    BsonDbPointer readDBPointer();

    /**
     * Reads a BSON DBPointer element from the reader.
     *
     * @param name The name of the element.
     * @return A DBPointer.
     */
    BsonDbPointer readDBPointer(String name);

    /**
     * Reads the start of a BSON array.
     */
    void readStartArray();

    /**
     * Reads the start of a BSON document.
     */
    void readStartDocument();

    /**
     * Reads a BSON String from the reader.
     *
     * @return A String.
     */
    String readString();

    /**
     * Reads a BSON string element from the reader.
     *
     * @param name The name of the element.
     * @return A String.
     */
    String readString(String name);

    /**
     * Reads a BSON symbol from the reader.
     *
     * @return A string.
     */
    String readSymbol();

    /**
     * Reads a BSON symbol element from the reader.
     *
     * @param name The name of the element.
     * @return A string.
     */
    String readSymbol(String name);

    /**
     * Reads a BSON timestamp from the reader.
     *
     * @return The combined timestamp/increment.
     */
    BsonTimestamp readTimestamp();

    /**
     * Reads a BSON timestamp element from the reader.
     *
     * @param name The name of the element.
     * @return The combined timestamp/increment.
     */
    BsonTimestamp readTimestamp(String name);

    /**
     * Reads a BSON undefined from the reader.
     */
    void readUndefined();

    /**
     * Reads a BSON undefined element from the reader.
     *
     * @param name The name of the element.
     */
    void readUndefined(String name);

    /**
     * Skips the name (reader must be positioned on a name).
     */
    void skipName();

    /**
     * Skips the value (reader must be positioned on a value).
     */
    void skipValue();

    /**
     * Gets a mark representing the current state of the reader.
     *
     * @return the mark
     * @since 3.5
     */
    BsonReaderMark getMark();

    @Override
    void close();
}
