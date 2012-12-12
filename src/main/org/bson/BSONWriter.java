/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.bson;

import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.bson.util.StringUtils;

import java.io.Closeable;
import java.util.Arrays;

public abstract class BSONWriter implements Closeable {
    private BsonWriterSettings settings;
    private State state;
    private String name;
    private boolean checkElementNames;
    private boolean checkUpdateDocument;
    private int serializationDepth;
    private boolean closed;

    // constructors
    /// <summary>
    /// Initializes a new instance of the BsonWriter class.
    /// </summary>
    /// <param name="settings">The writer settings.</param>
    protected BSONWriter(BsonWriterSettings settings) {
        this.settings = settings;
        state = State.INITIAL;
    }

    protected String getName() {
        return name;
    }

    protected boolean isClosed() {
        return closed;
    }

    protected void setState(State state) {
        this.state = state;
    }

    protected State getState() {
        return state;
    }

//    // public static methods

//    /// <summary>
//    /// Creates a BsonWriter to a BsonBuffer.
//    /// </summary>
//    /// <param name="settings">Optional BsonBinaryWriterSettings.</param>
//    /// <returns>A BsonWriter.</returns>
//    public static BsonWriter Create(BsonBinaryWriterSettings settings)
//    {
//        return new BsonBinaryWriter(null, null, settings);
//    }
//
//    /// <summary>
//    /// Creates a BsonWriter to a BsonBuffer.
//    /// </summary>
//    /// <param name="buffer">A BsonBuffer.</param>
//    /// <returns>A BsonWriter.</returns>
//    public static BsonWriter Create(BsonBuffer buffer)
//    {
//        return new BsonBinaryWriter(null, buffer, BsonBinaryWriterSettings.Defaults);
//    }
//
//    /// <summary>
//    /// Creates a BsonWriter to a BsonBuffer.
//    /// </summary>
//    /// <param name="buffer">A BsonBuffer.</param>
//    /// <param name="settings">Optional BsonBinaryWriterSettings.</param>
//    /// <returns>A BsonWriter.</returns>
//    public static BsonWriter Create(BsonBuffer buffer, BsonBinaryWriterSettings settings)
//    {
//        return new BsonBinaryWriter(null, buffer, settings);
//    }
//
//    /// <summary>
//    /// Creates a BsonWriter to a BsonDocument.
//    /// </summary>
//    /// <param name="document">A BsonDocument.</param>
//    /// <returns>A BsonWriter.</returns>
//    public static BsonWriter Create(BsonDocument document)
//    {
//        return Create(document, BsonDocumentWriterSettings.Defaults);
//    }
//
//    /// <summary>
//    /// Creates a BsonWriter to a BsonDocument.
//    /// </summary>
//    /// <param name="document">A BsonDocument.</param>
//    /// <param name="settings">The settings.</param>
//    /// <returns>A BsonWriter.</returns>
//    public static BsonWriter Create(BsonDocument document, BsonDocumentWriterSettings settings)
//    {
//        return new BsonDocumentWriter(document, settings);
//    }
//
//    /// <summary>
//    /// Creates a BsonWriter to a BSON Stream.
//    /// </summary>
//    /// <param name="stream">A Stream.</param>
//    /// <returns>A BsonWriter.</returns>
//    public static BsonWriter Create(Stream stream)
//    {
//        return Create(stream, BsonBinaryWriterSettings.Defaults);
//    }
//
//    /// <summary>
//    /// Creates a BsonWriter to a BSON Stream.
//    /// </summary>
//    /// <param name="stream">A Stream.</param>
//    /// <param name="settings">Optional BsonBinaryWriterSettings.</param>
//    /// <returns>A BsonWriter.</returns>
//    public static BsonWriter Create(Stream stream, BsonBinaryWriterSettings settings)
//    {
//        return new BsonBinaryWriter(stream, null, BsonBinaryWriterSettings.Defaults);
//    }
//
//    /// <summary>
//    /// Creates a BsonWriter to a JSON TextWriter.
//    /// </summary>
//    /// <param name="writer">A TextWriter.</param>
//    /// <returns>A BsonWriter.</returns>
//    public static BsonWriter Create(TextWriter writer)
//    {
//        return new JsonWriter(writer, JsonWriterSettings.Defaults);
//    }
//
//    /// <summary>
//    /// Creates a BsonWriter to a JSON TextWriter.
//    /// </summary>
//    /// <param name="writer">A TextWriter.</param>
//    /// <param name="settings">Optional JsonWriterSettings.</param>
//    /// <returns>A BsonWriter.</returns>
//    public static BsonWriter Create(TextWriter writer, JsonWriterSettings settings)
//    {
//        return new JsonWriter(writer, settings);
//    }

    /// <summary>
    /// Flushes any pending data to the output destination.
    /// </summary>
    public abstract void flush();

    /// <summary>
    /// Writes a BSON binary data element to the writer.
    /// </summary>
    /// <param name="bytes">The binary data.</param>
    /// <param name="subType">The binary data subtype.</param>
    public abstract void writeBinaryData(Binary binary);

    /// <summary>
    /// Writes a BSON binary data element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="bytes">The binary data.</param>
    /// <param name="subType">The binary data subtype.</param>
    public void writeBinaryData(String name, Binary binary) {
        writeName(name);
        writeBinaryData(binary);
    }

    /// <summary>
    /// Writes a BSON Boolean to the writer.
    /// </summary>
    /// <param name="value">The Boolean value.</param>
    public abstract void writeBoolean(boolean value);

    /// <summary>
    /// Writes a BSON Boolean element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="value">The Boolean value.</param>
    public void writeBoolean(String name, boolean value) {
        writeName(name);
        writeBoolean(value);
    }

    /// <summary>
    /// Writes a BSON DateTime to the writer.
    /// </summary>
    /// <param name="value">The number of milliseconds since the Unix epoch.</param>
    public abstract void writeDateTime(long value);

    /// <summary>
    /// Writes a BSON DateTime element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="value">The number of milliseconds since the Unix epoch.</param>
    public void writeDateTime(String name, long value) {
        writeName(name);
        writeDateTime(value);
    }

    /// <summary>
    /// Writes a BSON Double to the writer.
    /// </summary>
    /// <param name="value">The Double value.</param>
    public abstract void writeDouble(double value);

    /// <summary>
    /// Writes a BSON Double element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="value">The Double value.</param>
    public void writeDouble(String name, double value) {
        writeName(name);
        writeDouble(value);
    }

    /// <summary>
    /// Writes the end of a BSON array to the writer.
    /// </summary>
    public void writeEndArray() {
        serializationDepth--;
    }

    /// <summary>
    /// Writes the end of a BSON document to the writer.
    /// </summary>
    public void writeEndDocument() {
        serializationDepth--;
    }

    /// <summary>
    /// Writes a BSON INT32 to the writer.
    /// </summary>
    /// <param name="value">The INT32 value.</param>
    public abstract void writeInt32(int value);

    /// <summary>
    /// Writes a BSON INT32 element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="value">The INT32 value.</param>
    public void writeInt32(String name, int value) {
        writeName(name);
        writeInt32(value);
    }

    /// <summary>
    /// Writes a BSON Int64 to the writer.
    /// </summary>
    /// <param name="value">The Int64 value.</param>
    public abstract void writeInt64(long value);

    /// <summary>
    /// Writes a BSON Int64 element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="value">The Int64 value.</param>
    public void writeInt64(String name, long value) {
        writeName(name);
        writeInt64(value);
    }

    /// <summary>
    /// Writes a BSON JavaScript to the writer.
    /// </summary>
    /// <param name="code">The JavaScript code.</param>
    public abstract void writeJavaScript(String code);

    /// <summary>
    /// Writes a BSON JavaScript element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="code">The JavaScript code.</param>
    public void writeJavaScript(String name, String code) {
        writeName(name);
        writeJavaScript(code);
    }

    /// <summary>
    /// Writes a BSON JavaScript to the writer (call WriteStartDocument to start writing the scope).
    /// </summary>
    /// <param name="code">The JavaScript code.</param>
    public abstract void writeJavaScriptWithScope(String code);

    /// <summary>
    /// Writes a BSON JavaScript element to the writer (call WriteStartDocument to start writing the scope).
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="code">The JavaScript code.</param>
    public void writeJavaScriptWithScope(String name, String code) {
        writeName(name);
        writeJavaScriptWithScope(code);
    }

    /// <summary>
    /// Writes a BSON MaxKey to the writer.
    /// </summary>
    public abstract void writeMaxKey();

    /// <summary>
    /// Writes a BSON MaxKey element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void writeMaxKey(String name) {
        writeName(name);
        writeMaxKey();
    }

    /// <summary>
    /// Writes a BSON MinKey to the writer.
    /// </summary>
    public abstract void writeMinKey();

    /// <summary>
    /// Writes a BSON MinKey element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void writeMinKey(String name) {
        writeName(name);
        writeMinKey();
    }

    /// <summary>
    /// Writes the name of an element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void writeName(String name) {
        if (state != State.NAME) {
            throwInvalidState("WriteName", State.NAME);
        }
        checkElementName(name);

        this.name = name;
        state = State.VALUE;
    }

    /// <summary>
    /// Writes a BSON null to the writer.
    /// </summary>
    public abstract void writeNull();

    /// <summary>
    /// Writes a BSON null element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void writeNull(String name) {
        writeName(name);
        writeNull();
    }

    /// <summary>
    /// Writes a BSON ObjectId to the writer.
    /// </summary>
    /// <param name="timestamp">The timestamp.</param>
    /// <param name="machine">The machine hash.</param>
    /// <param name="pid">The PID.</param>
    /// <param name="increment">The increment.</param>
    public abstract void writeObjectId(ObjectId objectId);

    /// <summary>
    /// Writes a BSON ObjectId element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="timestamp">The timestamp.</param>
    /// <param name="machine">The machine hash.</param>
    /// <param name="pid">The PID.</param>
    /// <param name="increment">The increment.</param>
    public void writeObjectId(String name, ObjectId objectId) {
        writeName(name);
        writeObjectId(objectId);
    }

    /// <summary>
    /// Writes a BSON regular expression to the writer.
    /// </summary>
    public abstract void writeRegularExpression(RegularExpression regularExpression);

    /// <summary>
    /// Writes a BSON regular expression element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void writeRegularExpression(String name, RegularExpression regularExpression) {
        writeName(name);
        writeRegularExpression(regularExpression);
    }

    /// <summary>
    /// Writes the start of a BSON array to the writer.
    /// </summary>
    public void writeStartArray() {
        serializationDepth++;
        if (serializationDepth > settings.maxSerializationDepth) {
            throw new BsonSerializationException("Maximum serialization depth exceeded (does the object being serialized have a circular reference?).");
        }
    }

    /// <summary>
    /// Writes the start of a BSON array element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void writeStartArray(String name) {
        writeName(name);
        writeStartArray();
    }

    /// <summary>
    /// Writes the start of a BSON document to the writer.
    /// </summary>
    public void writeStartDocument() {
        serializationDepth++;
        if (serializationDepth > settings.maxSerializationDepth) {
            throw new BsonSerializationException("Maximum serialization depth exceeded (does the object being serialized have a circular reference?).");
        }
    }

    /// <summary>
    /// Writes the start of a BSON document element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void writeStartDocument(String name) {
        writeName(name);
        writeStartDocument();
    }

    /// <summary>
    /// Writes a BSON String to the writer.
    /// </summary>
    /// <param name="value">The String value.</param>
    public abstract void writeString(String value);

    /// <summary>
    /// Writes a BSON String element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="value">The String value.</param>
    public void writeString(String name, String value) {
        writeName(name);
        writeString(value);
    }

    /// <summary>
    /// Writes a BSON Symbol to the writer.
    /// </summary>
    /// <param name="value">The symbol.</param>
    public abstract void writeSymbol(String value);

    /// <summary>
    /// Writes a BSON Symbol element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="value">The symbol.</param>
    public void writeSymbol(String name, String value) {
        writeName(name);
        writeSymbol(value);
    }

    /// <summary>
    /// Writes a BSON timestamp to the writer.
    /// </summary>
    /// <param name="value">The combined timestamp/increment value.</param>
    public abstract void writeTimestamp(long value);

    /// <summary>
    /// Writes a BSON timestamp element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="value">The combined timestamp/increment value.</param>
    public void writeTimestamp(String name, long value) {
        writeName(name);
        writeTimestamp(value);
    }

    /// <summary>
    /// Writes a BSON undefined to the writer.
    /// </summary>
    public abstract void writeUndefined();

    /// <summary>
    /// Writes a BSON undefined element to the writer.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void writeUndefined(String name) {
        writeName(name);
        writeUndefined();
    }

    // protected methods
    /// <summary>
    /// Checks that the element name is valid.
    /// </summary>
    /// <param name="name">The element name to be checked.</param>
    protected void checkElementName(String name) {
        if (checkUpdateDocument) {
            checkElementNames = (name.charAt(0) != '$');
            checkUpdateDocument = false;
            return;
        }

        if (checkElementNames) {
            if (name.charAt(0) == '$') {
                // a few element names starting with $ have to be allowed for historical reasons
                if (!(name.equals("$code") || name.equals("$db") || name.equals("$ref") || name.equals("$scope") || name.equals("$id"))) {
                    String message = String.format("Element name '%s' is not valid because it starts with a '$'.", name);
                    throw new BsonSerializationException(message);
                }
            }
            if (name.indexOf('.') != -1) {
                String message = String.format("Element name '%s' is not valid because it contains a '.'.", name);
                throw new BsonSerializationException(message);
            }
        }
    }

    /// <summary>
    /// Throws an InvalidOperationException when the method called is not valid for the current ContextType.
    /// </summary>
    /// <param name="methodName">The name of the method.</param>
    /// <param name="actualContextType">The actual ContextType.</param>
    /// <param name="validContextTypes">The valid ContextTypes.</param>
    protected void throwInvalidContextType(String methodName, ContextType actualContextType,
                                           ContextType... validContextTypes) {
        String validContextTypesString = StringUtils.join(" or ", Arrays.asList(validContextTypes));
        String message = String.format(
                "%s can only be called when ContextType is %s, not when ContextType is %s.",
                methodName, validContextTypesString, actualContextType);
        throw new InvalidOperationException(message);
    }

    /// <summary>
    /// Throws an InvalidOperationException when the method called is not valid for the current state.
    /// </summary>
    /// <param name="methodName">The name of the method.</param>
    /// <param name="validStates">The valid states.</param>
    protected void throwInvalidState(String methodName, State... validStates) {
        String message;
        if (state == State.INITIAL || state == State.SCOPE_DOCUMENT || state == State.DONE) {
            if (!methodName.startsWith("end") && !methodName.equals("writeName")) {
                String typeName = methodName.substring(5);
                if (typeName.startsWith("start")) {
                    typeName = typeName.substring(5);
                }
                String article = "A";
                if (Arrays.asList('A', 'E', 'I', 'O', 'U').contains(typeName.charAt(0))) {
                    article = "An";
                }
                message = String.format(
                        "%s %s value cannot be written to the root level of a BSON document.",
                        article, typeName);
                throw new InvalidOperationException(message);
            }
        }

        String validStatesString = StringUtils.join(" or ", Arrays.asList(validStates));
        message = String.format(
                "%s can only be called when State is %s, not when State is %s",
                methodName, validStatesString, state);
        throw new InvalidOperationException(message);
    }

    public void close() {
        closed = true;
    }

    protected enum State {
        // The initial state.
        INITIAL,
        // The writer is positioned to write a name.
        NAME,
        // The writer is positioned to write a value.
        VALUE,
        // The writer is positioned to write a scope document (call WriteStartDocument to start writing the scope document).
        SCOPE_DOCUMENT,
        // The writer is done.
        DONE,
        // The writer is closed.
        CLOSED
    }
}
