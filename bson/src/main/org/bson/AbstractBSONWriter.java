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

package org.bson;

import org.bson.types.Binary;
import org.bson.types.DBPointer;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.bson.types.Timestamp;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Stack;

import static java.lang.String.format;

/**
 * Represents a BSON writer for some external format (see subclasses).
 *
 * @since 3.0.0
 */
public abstract class AbstractBSONWriter implements BSONWriter, Closeable {
    private final BSONWriterSettings settings;
    private final Stack<FieldNameValidator> fieldNameValidatorStack = new Stack<FieldNameValidator>();
    private State state;
    private Context context;
    private int serializationDepth;
    private boolean closed;

    /**
     * Initializes a new instance of the BSONWriter class.
     *
     * @param settings The writer settings.
     */
    protected AbstractBSONWriter(final BSONWriterSettings settings) {
        this(settings, new NoOpFieldNameValidator());
    }

    /**
     * Initializes a new instance of the BSONWriter class.
     *
     * @param settings  The writer settings.
     * @param validator the field name validator
     */
    protected AbstractBSONWriter(final BSONWriterSettings settings, final FieldNameValidator validator) {
        if (validator == null) {
            throw new IllegalArgumentException("Validator can not be null");
        }
        this.settings = settings;
        fieldNameValidatorStack.push(validator);
        state = State.INITIAL;
    }

    protected String getName() {
        return context.name;
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

    @Override
    public void writeBinaryData(final String name, final Binary binary) {
        writeName(name);
        writeBinaryData(binary);
    }

    @Override
    public void writeBoolean(final String name, final boolean value) {
        writeName(name);
        writeBoolean(value);
    }

    @Override
    public void writeDateTime(final String name, final long value) {
        writeName(name);
        writeDateTime(value);
    }

    @Override
    public void writeDouble(final String name, final double value) {
        writeName(name);
        writeDouble(value);
    }

    @Override
    public void writeEndArray() {
        if (context.getParentContext() != null && context.getParentContext().name != null) {
            fieldNameValidatorStack.pop();
        }
        serializationDepth--;
    }

    @Override
    public void writeEndDocument() {
        if (context.getParentContext() != null && context.getParentContext().name != null) {
            fieldNameValidatorStack.pop();
        }
        serializationDepth--;
    }

    @Override
    public void writeInt32(final String name, final int value) {
        writeName(name);
        writeInt32(value);
    }

    @Override
    public void writeInt64(final String name, final long value) {
        writeName(name);
        writeInt64(value);
    }

    @Override
    public void writeJavaScript(final String name, final String code) {
        writeName(name);
        writeJavaScript(code);
    }

    @Override
    public void writeJavaScriptWithScope(final String name, final String code) {
        writeName(name);
        writeJavaScriptWithScope(code);
    }

    @Override
    public void writeMaxKey(final String name) {
        writeName(name);
        writeMaxKey();
    }

    @Override
    public void writeMinKey(final String name) {
        writeName(name);
        writeMinKey();
    }

    @Override
    public void writeName(final String name) {
        if (state != State.NAME) {
            throwInvalidState("WriteName", State.NAME);
        }
        if (name == null) {
            throw new IllegalArgumentException("BSON field name can not be null");
        }
        if (!fieldNameValidatorStack.peek().validate(name)) {
            throw new IllegalArgumentException(String.format("Invalid BSON field name %s", name));
        }
        context.name = name;
        state = State.VALUE;
    }

    @Override
    public void writeNull(final String name) {
        writeName(name);
        writeNull();
    }

    @Override
    public void writeObjectId(final String name, final ObjectId objectId) {
        writeName(name);
        writeObjectId(objectId);
    }

    @Override
    public void writeRegularExpression(final String name, final RegularExpression regularExpression) {
        writeName(name);
        writeRegularExpression(regularExpression);
    }

    @Override
    public void writeStartArray() {
        if (context != null && context.name != null) {
            fieldNameValidatorStack.push(fieldNameValidatorStack.peek().getValidatorForField(getName()));
        }
        serializationDepth++;
        if (serializationDepth > settings.getMaxSerializationDepth()) {
            throw new BSONSerializationException("Maximum serialization depth exceeded (does the object being "
                                                 + "serialized have a circular reference?).");
        }
    }

    @Override
    public void writeStartArray(final String name) {
        writeName(name);
        writeStartArray();
    }

    @Override
    public void writeStartDocument() {
        if (context != null && context.name != null) {
            fieldNameValidatorStack.push(fieldNameValidatorStack.peek().getValidatorForField(getName()));
        }
        serializationDepth++;
        if (serializationDepth > settings.getMaxSerializationDepth()) {
            throw new BSONSerializationException("Maximum serialization depth exceeded (does the object being "
                                                 + "serialized have a circular reference?).");
        }
    }

    @Override
    public void writeStartDocument(final String name) {
        writeName(name);
        writeStartDocument();
    }

    @Override
    public void writeString(final String name, final String value) {
        writeName(name);
        writeString(value);
    }

    @Override
    public void writeSymbol(final String name, final String value) {
        writeName(name);
        writeSymbol(value);
    }

    @Override
    public void writeTimestamp(final String name, final Timestamp value) {
        writeName(name);
        writeTimestamp(value);
    }

    @Override
    public void writeUndefined(final String name) {
        writeName(name);
        writeUndefined();
    }

    private void writeDBPointer(final DBPointer dbPointer) {
        writeStartDocument();
        writeString("$ref", dbPointer.getNamespace());
        writeObjectId("$id", dbPointer.getId());
        writeEndDocument();
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
     * @throws BSONInvalidOperationException
     */
    protected void throwInvalidContextType(final String methodName, final BSONContextType actualContextType,
                                           final BSONContextType... validContextTypes) {
        String validContextTypesString = StringUtils.join(" or ", Arrays.asList(validContextTypes));
        String message = format("%s can only be called when ContextType is %s, "
                                + "not when ContextType is %s.", methodName, validContextTypesString,
                                actualContextType);
        throw new BSONInvalidOperationException(message);
    }

    /**
     * Throws an InvalidOperationException when the method called is not valid for the current state.
     *
     * @param methodName  The name of the method.
     * @param validStates The valid states.
     * @throws BSONInvalidOperationException
     */
    protected void throwInvalidState(final String methodName, final State... validStates) {
        String message;
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
                throw new BSONInvalidOperationException(message);
            }
        }

        String validStatesString = StringUtils.join(" or ", Arrays.asList(validStates));
        message = format("%s can only be called when State is %s, not when State is %s", methodName,
                         validStatesString, state);
        throw new BSONInvalidOperationException(message);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void pipe(final BSONReader reader) {
        pipeDocument(reader);
    }

    private void pipeDocument(final BSONReader reader) {
        reader.readStartDocument();
        writeStartDocument();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            writeName(reader.readName());
            pipeValue(reader);
        }
        reader.readEndDocument();
        writeEndDocument();
    }

    private void pipeArray(final BSONReader reader) {
        reader.readStartArray();
        writeStartArray();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            pipeValue(reader);
        }
        reader.readEndArray();
        writeEndArray();
    }

    private void pipeJavascriptWithScope(final BSONReader reader) {
        writeJavaScriptWithScope(reader.readJavaScriptWithScope());
        pipeDocument(reader);
    }

    private void pipeValue(final BSONReader reader) {
        switch (reader.getCurrentBSONType()) {
            case DOCUMENT:
                pipeDocument(reader);
                break;
            case ARRAY:
                pipeArray(reader);
                break;
            case DOUBLE:
                writeDouble(reader.readDouble());
                break;
            case STRING:
                writeString(reader.readString());
                break;
            case BINARY:
                writeBinaryData(reader.readBinaryData());
                break;
            case UNDEFINED:
                reader.readUndefined();
                writeUndefined();
                break;
            case OBJECT_ID:
                writeObjectId(reader.readObjectId());
                break;
            case BOOLEAN:
                writeBoolean(reader.readBoolean());
                break;
            case DATE_TIME:
                writeDateTime(reader.readDateTime());
                break;
            case NULL:
                reader.readNull();
                writeNull();
                break;
            case REGULAR_EXPRESSION:
                writeRegularExpression(reader.readRegularExpression());
                break;
            case JAVASCRIPT:
                writeJavaScript(reader.readJavaScript());
                break;
            case SYMBOL:
                writeSymbol(reader.readSymbol());
                break;
            case JAVASCRIPT_WITH_SCOPE:
                pipeJavascriptWithScope(reader);
                break;
            case INT32:
                writeInt32(reader.readInt32());
                break;
            case TIMESTAMP:
                writeTimestamp(reader.readTimestamp());
                break;
            case INT64:
                writeInt64(reader.readInt64());
                break;
            case MIN_KEY:
                reader.readMinKey();
                writeMinKey();
                break;
            case DB_POINTER:
                writeDBPointer(reader.readDBPointer());
                break;
            case MAX_KEY:
                reader.readMaxKey();
                writeMaxKey();
                break;
            default:
                throw new IllegalArgumentException("unhandled BSON type: " + reader.getCurrentBSONType());
        }
    }

    public enum State {
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

    public class Context {
        private final Context parentContext;
        private final BSONContextType contextType;
        private String name;

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

    protected class Mark {
        private final Context markedContext;
        private final State markedState;
        private final String currentName;
        private final int serializationDepth;

        protected Mark() {
            this.markedContext = AbstractBSONWriter.this.context.copy();
            this.markedState = AbstractBSONWriter.this.state;
            this.currentName = AbstractBSONWriter.this.context.name;
            this.serializationDepth = AbstractBSONWriter.this.serializationDepth;
        }

        protected void reset() {
            setContext(markedContext);
            setState(markedState);
            AbstractBSONWriter.this.context.name = currentName;
            AbstractBSONWriter.this.serializationDepth = serializationDepth;
        }
    }
}