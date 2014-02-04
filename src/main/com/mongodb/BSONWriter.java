/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import org.bson.BSONException;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.io.Closeable;
import java.util.Arrays;

import static java.lang.String.format;

abstract class BSONWriter implements Closeable {
    private final BSONWriterSettings settings;
    private State state;
    private Context context;
    private String currentName;
    private int serializationDepth;
    private boolean closed;

    /**
     * Initializes a new instance of the BSONWriter class.
     *
     * @param settings The writer settings.
     */
    protected BSONWriter(final BSONWriterSettings settings) {
        this.settings = settings;
        state = State.INITIAL;
    }

    protected String getName() {
        return currentName;
    }

    protected boolean isClosed() {
        return closed;
    }

    protected void setState(final State state) {
        this.state = state;
    }

    protected State getState() {
        return state;
    }

    protected Context getContext() {
        return context;
    }

    protected void setContext(final Context context) {
        this.context = context;
    }

    /**
     * Flushes any pending data to the output destination.
     */
    public abstract void flush();

    /**
     * Writes a BSON Binary data element to the writer.
     *
     * @param binary The Binary data.
     */
    public abstract void writeBinaryData(Binary binary);

    /**
     * Writes a BSON Binary data element to the writer.
     *
     * @param name   The name of the element.
     * @param binary The Binary data value.
     */
    public void writeBinaryData(final String name, final Binary binary) {
        writeName(name);
        writeBinaryData(binary);
    }

    /**
     * Writes a BSON Boolean to the writer.
     *
     * @param value The Boolean value.
     */
    public abstract void writeBoolean(boolean value);

    /**
     * Writes a BSON Boolean element to the writer.
     *
     * @param name  The name of the element.
     * @param value The Boolean value.
     */
    public void writeBoolean(final String name, final boolean value) {
        writeName(name);
        writeBoolean(value);
    }

    /**
     * Writes a BSON DateTime to the writer.
     *
     * @param value The number of milliseconds since the Unix epoch.
     */
    public abstract void writeDateTime(long value);

    /**
     * Writes a BSON DateTime element to the writer.
     *
     * @param name  The name of the element.
     * @param value The number of milliseconds since the Unix epoch.
     */
    public void writeDateTime(final String name, final long value) {
        writeName(name);
        writeDateTime(value);
    }

    /**
     * Writes a BSON Double to the writer.
     *
     * @param value The Double value.
     */
    public abstract void writeDouble(double value);

    /**
     * Writes a BSON Double element to the writer.
     *
     * @param name  The name of the element.
     * @param value The Double value.
     */
    public void writeDouble(final String name, final double value) {
        writeName(name);
        writeDouble(value);
    }

    /**
     * Writes the end of a BSON array to the writer.
     */
    public void writeEndArray() {
        serializationDepth--;
    }

    /**
     * Writes the end of a BSON document to the writer.
     */
    public void writeEndDocument() {
        serializationDepth--;
    }

    /**
     * Writes a BSON Int32 to the writer.
     *
     * @param value The Int32 value.
     */
    public abstract void writeInt32(int value);

    /**
     * Writes a BSON Int32 element to the writer.
     *
     * @param name  The name of the element.
     * @param value The Int32 value.
     */
    public void writeInt32(final String name, final int value) {
        writeName(name);
        writeInt32(value);
    }

    /**
     * Writes a BSON Int64 to the writer.
     *
     * @param value The Int64 value.
     */
    public abstract void writeInt64(long value);

    /**
     * Writes a BSON Int64 element to the writer.
     *
     * @param name  The name of the element.
     * @param value The Int64 value.
     */
    public void writeInt64(final String name, final long value) {
        writeName(name);
        writeInt64(value);
    }

    /**
     * Writes a BSON JavaScript to the writer.
     *
     * @param code The JavaScript code.
     */
    public abstract void writeJavaScript(String code);

    /**
     * Writes a BSON JavaScript element to the writer.
     *
     * @param name The name of the element.
     * @param code The JavaScript code.
     */
    public void writeJavaScript(final String name, final String code) {
        writeName(name);
        writeJavaScript(code);
    }

    /**
     * Writes a BSON JavaScript to the writer (call WriteStartDocument to start writing the scope).
     *
     * @param code The JavaScript code.
     */
    public abstract void writeJavaScriptWithScope(String code);

    /**
     * Writes a BSON JavaScript element to the writer (call WriteStartDocument to start writing the scope).
     *
     * @param name The name of the element.
     * @param code The JavaScript code.
     */
    public void writeJavaScriptWithScope(final String name, final String code) {
        writeName(name);
        writeJavaScriptWithScope(code);
    }

    /**
     * Writes a BSON MaxKey to the writer.
     */
    public abstract void writeMaxKey();

    /**
     * Writes a BSON MaxKey element to the writer.
     *
     * @param name The name of the element.
     */
    public void writeMaxKey(final String name) {
        writeName(name);
        writeMaxKey();
    }

    /**
     * Writes a BSON MinKey to the writer.
     */
    public abstract void writeMinKey();

    /**
     * Writes a BSON MinKey element to the writer.
     *
     * @param name The name of the element.
     */
    public void writeMinKey(final String name) {
        writeName(name);
        writeMinKey();
    }

    /**
     * Writes the name of an element to the writer.
     *
     * @param name The name of the element.
     */
    public void writeName(final String name) {
        if (state != State.NAME) {
            throwInvalidState("WriteName", State.NAME);
        }
        this.currentName = name;
        state = State.VALUE;
    }

    /**
     * Writes a BSON null to the writer.
     */
    public abstract void writeNull();

    /**
     * Writes a BSON null element to the writer.
     *
     * @param name The name of the element.
     */
    public void writeNull(final String name) {
        writeName(name);
        writeNull();
    }

    /**
     * Writes a BSON ObjectId to the writer.
     *
     * @param objectId The ObjectId value.
     */
    public abstract void writeObjectId(ObjectId objectId);

    /**
     * Writes a BSON ObjectId element to the writer.
     *
     * @param name     The name of the element.
     * @param objectId The ObjectId value.
     */
    public void writeObjectId(final String name, final ObjectId objectId) {
        writeName(name);
        writeObjectId(objectId);
    }

//    /**
//     * Writes a BSON regular expression to the writer.
//     */
//    public abstract void writeRegularExpression(RegularExpression regularExpression);
//
//    /**
//     * Writes a BSON regular expression element to the writer.
//     *
//     * @param name              The name of the element.
//     * @param regularExpression The RegularExpression value.
//     */
//    public void writeRegularExpression(final String name, final RegularExpression regularExpression) {
//        writeName(name);
//        writeRegularExpression(regularExpression);
//    }
//
    /**
     * Writes the start of a BSON array to the writer.
     *
     * @throws BSONException if maximum serialization depth exceeded.
     */
    public void writeStartArray() {
        serializationDepth++;
        if (serializationDepth > settings.getMaxSerializationDepth()) {
            throw new BSONException("Maximum serialization depth exceeded (does the object being serialized have a circular reference?).");
        }
    }

    /**
     * Writes the start of a BSON array element to the writer.
     *
     * @param name The name of the element.
     */
    public void writeStartArray(final String name) {
        writeName(name);
        writeStartArray();
    }

    /**
     * Writes the start of a BSON document to the writer.
     *
     * @throws BSONException if maximum serialization depth exceeded.
     */
    public void writeStartDocument() {
        serializationDepth++;
        if (serializationDepth > settings.getMaxSerializationDepth()) {
            throw new BSONException("Maximum serialization depth exceeded (does the object being serialized have a circular reference?).");
        }
    }

    /**
     * Writes the start of a BSON document element to the writer.
     *
     * @param name The name of the element.
     */
    public void writeStartDocument(final String name) {
        writeName(name);
        writeStartDocument();
    }

    /**
     * Writes a BSON String to the writer.
     *
     * @param value The String value.
     */
    public abstract void writeString(String value);

    /**
     * Writes a BSON String element to the writer.
     *
     * @param name  The name of the element.
     * @param value The String value.
     */
    public void writeString(final String name, final String value) {
        writeName(name);
        writeString(value);
    }

    /**
     * Writes a BSON Symbol to the writer.
     *
     * @param value The symbol.
     */
    public abstract void writeSymbol(String value);

    /**
     * Writes a BSON Symbol element to the writer.
     *
     * @param name  The name of the element.
     * @param value The symbol.
     */
    public void writeSymbol(final String name, final String value) {
        writeName(name);
        writeSymbol(value);
    }

    /**
     * Writes a BSON Timestamp to the writer.
     *
     * @param value The combined timestamp/increment value.
     */
    public abstract void writeTimestamp(BSONTimestamp value);

    /**
     * Writes a BSON Timestamp element to the writer.
     *
     * @param name  The name of the element.
     * @param value The combined timestamp/increment value.
     */
    public void writeTimestamp(final String name, final BSONTimestamp value) {
        writeName(name);
        writeTimestamp(value);
    }

    /**
     * Writes a BSON undefined to the writer.
     */
    public abstract void writeUndefined();

    /**
     * Writes a BSON undefined element to the writer.
     *
     * @param name The name of the element.
     */
    public void writeUndefined(final String name) {
        writeName(name);
        writeUndefined();
    }

    protected State getNextState() {
        if (getContext().getContextType() == BSONContextType.ARRAY) {
            return State.VALUE;
        } else {
            return State.NAME;
        }
    }

    protected boolean checkState(final State[] validStates) {
        for (final State cur : validStates) {
            if (cur == getState()) {
                return true;
            }
        }
        return false;
    }

    protected void checkPreconditions(final String methodName, final State... validStates) {
        if (isClosed()) {
            throw new IllegalStateException("BSONWriter");
        }

        if (!checkState(validStates)) {
            throwInvalidState(methodName, validStates);
        }
    }

    /**
     * Throws an InvalidOperationException when the method called is not valid for the current ContextType.
     *
     * @param methodName        The name of the method.
     * @param actualContextType The actual ContextType.
     * @param validContextTypes The valid ContextTypes.
     * @throws BSONException
     */
    protected void throwInvalidContextType(final String methodName, final BSONContextType actualContextType,
                                           final BSONContextType... validContextTypes) {
        final String validContextTypesString = StringUtils.join(" or ", Arrays.asList(validContextTypes));
        final String message = format("%s can only be called when ContextType is %s, "
                                      + "not when ContextType is %s.", methodName, validContextTypesString,
                                      actualContextType);
        throw new BSONException(message);
    }

    /**
     * Throws an InvalidOperationException when the method called is not valid for the current state.
     *
     * @param methodName  The name of the method.
     * @param validStates The valid states.
     * @throws BSONException
     */
    protected void throwInvalidState(final String methodName, final State... validStates) {
        final String message;
        if (state == State.INITIAL || state == State.SCOPE_DOCUMENT || state == State.DONE) {
            if (!methodName.startsWith("end") && !methodName.equals("writeName")) { // NOPMD
                //NOPMD collapsing these if statements will not aid readability
                String typeName = methodName.substring(5);
                if (typeName.startsWith("start")) {
                    typeName = typeName.substring(5);
                }
                String article = "A";
                if (Arrays.asList('A', 'E', 'I', 'O', 'U').contains(typeName.charAt(0))) {
                    article = "An";
                }
                message = format("%s %s value cannot be written to the root level of a BSON document.", article,
                                 typeName);
                throw new BSONException(message);
            }
        }

        final String validStatesString = StringUtils.join(" or ", Arrays.asList(validStates));
        message = format("%s can only be called when State is %s, not when State is %s", methodName,
                         validStatesString, state);
        throw new BSONException(message);
    }

    /**
     * Closes the writer.
     */
    public void close() {
        closed = true;
    }

    enum State {
        /**
         * The initial state.
         */
        INITIAL,

        /**
         * The writer is positioned to write a name.
         */
        NAME,

        /**
         * The writer is positioned to write a value.
         */
        VALUE,

        /**
         * The writer is positioned to write a scope document (call WriteStartDocument to start writing the scope document).
         */
        SCOPE_DOCUMENT,

        /**
         * The writer is done.
         */
        DONE,

        /**
         * The writer is closed.
         */
        CLOSED
    }

    class Context {
        private final Context parentContext;
        private final BSONContextType contextType;

        public Context(final Context from) {
            parentContext = from.parentContext;
            contextType = from.contextType;
        }

        public Context(final Context parentContext, final BSONContextType contextType) {
            this.parentContext = parentContext;
            this.contextType = contextType;
        }

        public Context getParentContext() {
            return parentContext;
        }

        public BSONContextType getContextType() {
            return contextType;
        }

        public Context copy() {
            return new Context(this);
        }
    }

    class Mark {
        private final Context markedContext;
        private final State markedState;
        private final String currentName;
        private final int serializationDepth;

        protected Mark() {
            this.markedContext = BSONWriter.this.context.copy();
            this.markedState = BSONWriter.this.state;
            this.currentName = BSONWriter.this.currentName;
            this.serializationDepth = BSONWriter.this.serializationDepth;
        }

        protected void reset() {
            setContext(markedContext);
            setState(markedState);
            BSONWriter.this.currentName = currentName;
            BSONWriter.this.serializationDepth = serializationDepth;
        }
    }
}
