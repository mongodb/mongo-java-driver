/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.bson.json;

/**
 * An interface for creating JSON texts that largely conform to RFC 7159.
 *
 * @since 3.5
 */
public interface StrictJsonWriter {
    /**
     * Writes the name of a member to the writer.
     *
     * @param name the member name
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a member name
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeName(String name);

    /**
     * Writes a boolean to the writer.
     *
     * @param value the boolean value.
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a value
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeBoolean(boolean value);

    /**
     * Writes a a member with a boolean value to the writer.
     *
     * @param name  the member name
     * @param value the boolean value
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a member
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeBoolean(String name, boolean value);

    /**
     * Writes a number to the writer.
     *
     * @param value the Double value, as a String so that clients can take full control over formatting
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a value
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeNumber(String value);

    /**
     * Writes a member with a numeric value to the writer.
     *
     * @param name  the member name
     * @param value the Double value, as a String so that clients can take full control over formatting
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a member
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeNumber(String name, String value);

    /**
     * Writes a String to the writer.
     *
     * @param value the String value
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a value
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeString(String value);

    /**
     * Writes a member with a string value to the writer.
     *
     * @param name  the member name
     * @param value the String value
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a member
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeString(String name, String value);

    /**
     * Writes a raw value without quoting or escaping.
     *
     * @param value the String value
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a value
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeRaw(String value);

    /**
     * Writes a member with a raw value without quoting or escaping.
     *
     * @param name  the member name
     * @param value the raw value
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a member
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeRaw(String name, String value);

    /**
     * Writes a null value to the writer.
     *
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a value
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeNull();

    /**
     * Writes a member with a null value to the writer.
     *
     * @param name the member name
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a member
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeNull(String name);

    /**
     * Writes the start of a array to the writer.
     *
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a value
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeStartArray();

    /**
     * Writes the start of JSON array member to the writer.
     *
     * @param name the member name
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a member
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeStartArray(String name);

    /**
     * Writes the start of a JSON object to the writer.
     *
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a value
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeStartObject();

    /**
     * Writes the start of a JSON object member to the writer.
     *
     * @param name the member name
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write a member
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeStartObject(String name);

    /**
     * Writes the end of a JSON array to the writer.
     *
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write the end of an array
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeEndArray();

    /**
     * Writes the end of a JSON object to the writer.
     *
     * @throws org.bson.BsonInvalidOperationException if not in the correct state to write the end of an object
     * @throws org.bson.BSONException if the underlying Writer throws an IOException
     */
    void writeEndObject();

    /**
     * Return true if the output has been truncated due to exceeding any maximum length specified in settings.
     *
     * @return true if the output has been truncated
     * @since 3.7
     */
    boolean isTruncated();
}
