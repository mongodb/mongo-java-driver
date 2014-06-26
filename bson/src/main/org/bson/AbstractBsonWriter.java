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

import org.bson.types.ObjectId;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Stack;

import static java.lang.String.format;

/**
 * Represents a BSON writer for some external format (see subclasses).
 *
 * @since 3.0.0
 */
public abstract class AbstractBsonWriter implements BsonWriter, Closeable {
    private final BsonWriterSettings settings;
    private final Stack<FieldNameValidator> fieldNameValidatorStack = new Stack<FieldNameValidator>();
    private State state;
    private Context context;
    private int serializationDepth;
    private boolean closed;

    /**
     * Initializes a new instance of the BsonWriter class.
     *
     * @param settings The writer settings.
     */
    protected AbstractBsonWriter(final BsonWriterSettings settings) {
        this(settings, new NoOpFieldNameValidator());
    }

    /**
     * Initializes a new instance of the BsonWriter class.
     *
     * @param settings  The writer settings.
     * @param validator the field name validator
     */
    protected AbstractBsonWriter(final BsonWriterSettings settings, final FieldNameValidator validator) {
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

    protected abstract void doWriteStartDocument();
    protected abstract void doWriteEndDocument();
    protected abstract void doWriteStartArray();
    protected abstract void doWriteEndArray();
    protected abstract void doWriteBinaryData(final BsonBinary binary);
    protected abstract void doWriteBoolean(final boolean value);
    protected abstract void doWriteDateTime(final long value);
    protected abstract void doWriteDBPointer(final BsonDbPointer value);
    protected abstract void doWriteDouble(final double value);
    protected abstract void doWriteInt32(final int value);
    protected abstract void doWriteInt64(final long value);
    protected abstract void doWriteJavaScript(final String code);
    protected abstract void doWriteJavaScriptWithScope(final String code);
    protected abstract void doWriteMaxKey();
    protected abstract void doWriteMinKey();
    protected abstract void doWriteNull();
    protected abstract void doWriteObjectId(final ObjectId objectId);
    protected abstract void doWriteRegularExpression(final BsonRegularExpression regularExpression);
    protected abstract void doWriteString(final String value);
    protected abstract void doWriteSymbol(final String value);
    protected abstract void doWriteTimestamp(final BsonTimestamp value);
    protected abstract void doWriteUndefined();

    @Override
    public void writeStartDocument(final String name) {
        writeName(name);
        writeStartDocument();
    }

    @Override
    public void writeStartDocument() {
        checkPreconditions("writeStartDocument", State.INITIAL, State.VALUE, State.SCOPE_DOCUMENT, State.DONE);
        if (context != null && context.name != null) {
            fieldNameValidatorStack.push(fieldNameValidatorStack.peek().getValidatorForField(getName()));
        }
        serializationDepth++;
        if (serializationDepth > settings.getMaxSerializationDepth()) {
            throw new BsonSerializationException("Maximum serialization depth exceeded (does the object being "
                    + "serialized have a circular reference?).");
        }

        doWriteStartDocument();
        setState(State.NAME);
    }

    @Override
    public void writeEndDocument() {
        checkPreconditions("writeEndDocument", State.NAME);

        BsonContextType contextType = getContext().getContextType();
        if (contextType != BsonContextType.DOCUMENT && contextType != BsonContextType.SCOPE_DOCUMENT) {
            throwInvalidContextType("WriteEndDocument", contextType, BsonContextType.DOCUMENT, BsonContextType.SCOPE_DOCUMENT);
        }

        if (context.getParentContext() != null && context.getParentContext().name != null) {
            fieldNameValidatorStack.pop();
        }
        serializationDepth--;

        doWriteEndDocument();

        if (getContext() == null || getContext().getContextType() == BsonContextType.TOP_LEVEL) {
            setState(State.DONE);
        } else {
            setState(getNextState());
        }
    }

    @Override
    public void writeStartArray(final String name) {
        writeName(name);
        writeStartArray();
    }

    @Override
    public void writeStartArray() {
        checkPreconditions("writeStartArray", State.VALUE);

        if (context != null && context.name != null) {
            fieldNameValidatorStack.push(fieldNameValidatorStack.peek().getValidatorForField(getName()));
        }
        serializationDepth++;
        if (serializationDepth > settings.getMaxSerializationDepth()) {
            throw new BsonSerializationException("Maximum serialization depth exceeded (does the object being "
                    + "serialized have a circular reference?).");
        }

        doWriteStartArray();
        setState(State.VALUE);
    }

    @Override
    public void writeEndArray() {
        checkPreconditions("writeEndArray", State.VALUE);

        if (getContext().getContextType() != BsonContextType.ARRAY) {
            throwInvalidContextType("WriteEndArray", getContext().getContextType(), BsonContextType.ARRAY);
        }

        if (context.getParentContext() != null && context.getParentContext().name != null) {
            fieldNameValidatorStack.pop();
        }
        serializationDepth--;

        doWriteEndArray();
        setState(getNextState());
    }

    @Override
    public void writeBinaryData(final String name, final BsonBinary binary) {
        writeName(name);
        writeBinaryData(binary);
    }

    @Override
    public void writeBinaryData(final BsonBinary binary) {
        checkPreconditions("writeBinaryData", State.VALUE, State.INITIAL);
        doWriteBinaryData(binary);
        setState(getNextState());
    }

    @Override
    public void writeBoolean(final String name, final boolean value) {
        writeName(name);
        writeBoolean(value);
    }

    @Override
    public void writeBoolean(final boolean value) {
        checkPreconditions("writeBoolean", State.VALUE, State.INITIAL);
        doWriteBoolean(value);
        setState(getNextState());
    }

    @Override
    public void writeDateTime(final String name, final long value) {
        writeName(name);
        writeDateTime(value);
    }

    @Override
    public void writeDateTime(final long value) {
        checkPreconditions("writeDateTime", State.VALUE, State.INITIAL);
        doWriteDateTime(value);
        setState(getNextState());
    }

    @Override
    public void writeDBPointer(final String name, final BsonDbPointer value) {
        writeName(name);
        writeDBPointer(value);
    }

    @Override
    public void writeDBPointer(final BsonDbPointer value) {
        checkPreconditions("writeDBPointer", State.VALUE, State.INITIAL);
        doWriteDBPointer(value);
        setState(getNextState());
    }

    @Override
    public void writeDouble(final String name, final double value) {
        writeName(name);
        writeDouble(value);
    }

    @Override
    public void writeDouble(final double value) {
        checkPreconditions("writeDBPointer", State.VALUE, State.INITIAL);
        doWriteDouble(value);
        setState(getNextState());
    }

    @Override
    public void writeInt32(final String name, final int value) {
        writeName(name);
        writeInt32(value);
    }

    @Override
    public void writeInt32(final int value) {
        checkPreconditions("writeInt32", State.VALUE);
        doWriteInt32(value);
        setState(getNextState());
    }

    @Override
    public void writeInt64(final String name, final long value) {
        writeName(name);
        writeInt64(value);
    }

    @Override
    public void writeInt64(final long value) {
        checkPreconditions("writeInt64", State.VALUE);
        doWriteInt64(value);
        setState(getNextState());
    }

    @Override
    public void writeJavaScript(final String name, final String code) {
        writeName(name);
        writeJavaScript(code);
    }

    @Override
    public void writeJavaScript(final String code) {
        checkPreconditions("writeJavaScript", State.VALUE);
        doWriteJavaScript(code);
        setState(getNextState());
    }

    @Override
    public void writeJavaScriptWithScope(final String name, final String code) {
        writeName(name);
        writeJavaScriptWithScope(code);
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        checkPreconditions("writeJavaScriptWithScope", State.VALUE);
        doWriteJavaScriptWithScope(code);
        setState(State.SCOPE_DOCUMENT);
    }

    @Override
    public void writeMaxKey(final String name) {
        writeName(name);
        writeMaxKey();
    }

    @Override
    public void writeMaxKey() {
        checkPreconditions("writeMaxKey", State.VALUE);
        doWriteMaxKey();
        setState(getNextState());
    }

    @Override
    public void writeMinKey(final String name) {
        writeName(name);
        writeMinKey();
    }

    @Override
    public void writeMinKey() {
        checkPreconditions("writeMinKey", State.VALUE);
        doWriteMinKey();
        setState(getNextState());
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
    public void writeNull() {
        checkPreconditions("writeNull", State.VALUE);
        doWriteNull();
        setState(getNextState());
    }

    @Override
    public void writeObjectId(final String name, final ObjectId objectId) {
        writeName(name);
        writeObjectId(objectId);
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        checkPreconditions("writeObjectId", State.VALUE);
        doWriteObjectId(objectId);
        setState(getNextState());
    }

    @Override
    public void writeRegularExpression(final String name, final BsonRegularExpression regularExpression) {
        writeName(name);
        writeRegularExpression(regularExpression);
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        checkPreconditions("writeRegularExpression", State.VALUE);
        doWriteRegularExpression(regularExpression);
        setState(getNextState());
    }

    @Override
    public void writeString(final String name, final String value) {
        writeName(name);
        writeString(value);
    }

    @Override
    public void writeString(final String value) {
        checkPreconditions("writeString", State.VALUE);
        doWriteString(value);
        setState(getNextState());

    }

    @Override
    public void writeSymbol(final String name, final String value) {
        writeName(name);
        writeSymbol(value);
    }

    @Override
    public void writeSymbol(final String value) {
        checkPreconditions("writeSymbol", State.VALUE);
        doWriteSymbol(value);
        setState(getNextState());
    }

    @Override
    public void writeTimestamp(final String name, final BsonTimestamp value) {
        writeName(name);
        writeTimestamp(value);
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        checkPreconditions("writeTimestamp", State.VALUE);
        doWriteTimestamp(value);
        setState(getNextState());
    }

    @Override
    public void writeUndefined(final String name) {
        writeName(name);
        writeUndefined();
    }

    @Override
    public void writeUndefined() {
        checkPreconditions("writeUndefined", State.VALUE);
        doWriteUndefined();
        setState(getNextState());
    }

    protected State getNextState() {
        if (getContext().getContextType() == BsonContextType.ARRAY) {
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
            throw new IllegalStateException("BsonWriter");
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
     * @throws BsonInvalidOperationException
     */
    protected void throwInvalidContextType(final String methodName, final BsonContextType actualContextType,
                                           final BsonContextType... validContextTypes) {
        String validContextTypesString = StringUtils.join(" or ", Arrays.asList(validContextTypes));
        String message = format("%s can only be called when ContextType is %s, "
                                + "not when ContextType is %s.", methodName, validContextTypesString,
                                actualContextType);
        throw new BsonInvalidOperationException(message);
    }

    /**
     * Throws an InvalidOperationException when the method called is not valid for the current state.
     *
     * @param methodName  The name of the method.
     * @param validStates The valid states.
     * @throws BsonInvalidOperationException
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
                throw new BsonInvalidOperationException(message);
            }
        }

        String validStatesString = StringUtils.join(" or ", Arrays.asList(validStates));
        message = format("%s can only be called when State is %s, not when State is %s", methodName,
                         validStatesString, state);
        throw new BsonInvalidOperationException(message);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void pipe(final BsonReader reader) {
        pipeDocument(reader);
    }

    private void pipeDocument(final BsonReader reader) {
        reader.readStartDocument();
        writeStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            writeName(reader.readName());
            pipeValue(reader);
        }
        reader.readEndDocument();
        writeEndDocument();
    }

    private void pipeArray(final BsonReader reader) {
        reader.readStartArray();
        writeStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            pipeValue(reader);
        }
        reader.readEndArray();
        writeEndArray();
    }

    private void pipeJavascriptWithScope(final BsonReader reader) {
        writeJavaScriptWithScope(reader.readJavaScriptWithScope());
        pipeDocument(reader);
    }

    private void pipeValue(final BsonReader reader) {
        switch (reader.getCurrentBsonType()) {
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
                throw new IllegalArgumentException("unhandled BSON type: " + reader.getCurrentBsonType());
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

        state, /**
         * The writer is closed.
         */
        CLOSED
    }

    public class Context {
        private final Context parentContext;
        private final BsonContextType contextType;
        private String name;

        public Context(final Context from) {
            parentContext = from.parentContext;
            contextType = from.contextType;
        }

        public Context(final Context parentContext, final BsonContextType contextType) {
            this.parentContext = parentContext;
            this.contextType = contextType;
        }

        public Context getParentContext() {
            return parentContext;
        }

        public BsonContextType getContextType() {
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
            this.markedContext = AbstractBsonWriter.this.context.copy();
            this.markedState = AbstractBsonWriter.this.state;
            this.currentName = AbstractBsonWriter.this.context.name;
            this.serializationDepth = AbstractBsonWriter.this.serializationDepth;
        }

        protected void reset() {
            setContext(markedContext);
            setState(markedState);
            AbstractBsonWriter.this.context.name = currentName;
            AbstractBsonWriter.this.serializationDepth = serializationDepth;
        }
    }
}
