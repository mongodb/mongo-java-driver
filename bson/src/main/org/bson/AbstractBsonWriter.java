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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;

/**
 * Represents a BSON writer for some external format (see subclasses).
 *
 * @since 3.0
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

    /**
     * The name of the field being written.
     *
     * @return the name of the field
     */
    protected String getName() {
        return context.name;
    }

    /**
     * Returns whether this writer has been closed.
     *
     * @return true if the {@link #close()} method has been called.
     */
    protected boolean isClosed() {
        return closed;
    }

    /**
     * Sets the current state of the writer. The current state determines what sort of actions are valid for this writer at this time.
     *
     * @param state the state to set this writer to.
     */
    protected void setState(final State state) {
        this.state = state;
    }

    /**
     * Gets the current state of this writer.  The current state determines what sort of actions are valid for this writer at this time.
     *
     * @return the current state of the writer.
     */
    protected State getState() {
        return state;
    }

    /**
     * Get the context, which will indicate which state the writer is in, for example which part of a document it's currently writing.
     *
     * @return the current context.
     */
    protected Context getContext() {
        return context;
    }

    /**
     * Set the context, which will indicate which state the writer is in, for example which part of a document it's currently writing.
     *
     * @param context the new context for this writer
     */
    protected void setContext(final Context context) {
        this.context = context;
    }

    /**
     * Handles the logic to start writing a document
     */
    protected abstract void doWriteStartDocument();

    /**
     * Handles the logic of writing the end of a document
     */
    protected abstract void doWriteEndDocument();

    /**
     * Handles the logic to start writing an array
     */
    protected abstract void doWriteStartArray();

    /**
     * Handles the logic of writing the end of an array
     */
    protected abstract void doWriteEndArray();

    /**
     * Handles the logic of writing a {@code BsonBinary} value
     *
     * @param value the {@code BsonBinary} value to write
     */
    protected abstract void doWriteBinaryData(BsonBinary value);


    /**
     * Handles the logic of writing a boolean value
     *
     * @param value the {@code boolean} value to write
     */
    protected abstract void doWriteBoolean(boolean value);

    /**
     * Handles the logic of writing a date time value
     *
     * @param value the {@code long} value to write
     */
    protected abstract void doWriteDateTime(long value);

    /**
     * Handles the logic of writing a DbPointer value
     *
     * @param value the {@code BsonDbPointer} value to write
     */
    protected abstract void doWriteDBPointer(BsonDbPointer value);

    /**
     * Handles the logic of writing a Double value
     *
     * @param value the {@code double} value to write
     */
    protected abstract void doWriteDouble(double value);

    /**
     * Handles the logic of writing an int32 value
     *
     * @param value the {@code int} value to write
     */
    protected abstract void doWriteInt32(int value);

    /**
     * Handles the logic of writing an int64 value
     *
     * @param value the {@code long} value to write
     */
    protected abstract void doWriteInt64(long value);

    /**
     * Handles the logic of writing a Decimal128 value
     *
     * @param value the {@code Decimal128} value to write
     * @since 3.4
     */
    protected abstract void doWriteDecimal128(Decimal128 value);

    /**
     * Handles the logic of writing a JavaScript function
     *
     * @param value the {@code String} value to write
     */
    protected abstract void doWriteJavaScript(String value);

    /**
     * Handles the logic of writing a scoped JavaScript function
     *
     * @param value the {@code boolean} value to write
     */
    protected abstract void doWriteJavaScriptWithScope(String value);

    /**
     * Handles the logic of writing a Max key
     */
    protected abstract void doWriteMaxKey();

    /**
     * Handles the logic of writing a Min key
     */
    protected abstract void doWriteMinKey();

    /**
     * Handles the logic of writing a Null value
     */
    protected abstract void doWriteNull();

    /**
     * Handles the logic of writing an ObjectId
     *
     * @param value the {@code ObjectId} value to write
     */
    protected abstract void doWriteObjectId(ObjectId value);

    /**
     * Handles the logic of writing a regular expression
     *
     * @param value the {@code BsonRegularExpression} value to write
     */
    protected abstract void doWriteRegularExpression(BsonRegularExpression value);

    /**
     * Handles the logic of writing a String
     *
     * @param value the {@code String} value to write
     */
    protected abstract void doWriteString(String value);

    /**
     * Handles the logic of writing a Symbol
     *
     * @param value the {@code boolean} value to write
     */
    protected abstract void doWriteSymbol(String value);

    /**
     * Handles the logic of writing a timestamp
     *
     * @param value the {@code BsonTimestamp} value to write
     */
    protected abstract void doWriteTimestamp(BsonTimestamp value);

    /**
     * Handles the logic of writing an Undefined  value
     */
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
            FieldNameValidator validator = fieldNameValidatorStack.peek().getValidatorForField(getName());
            fieldNameValidatorStack.push(validator);
            validator.start();
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
            fieldNameValidatorStack.pop().end();
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
        notNull("name", name);
        notNull("value", binary);
        writeName(name);
        writeBinaryData(binary);
    }

    @Override
    public void writeBinaryData(final BsonBinary binary) {
        notNull("value", binary);
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
        notNull("name", name);
        notNull("value", value);
        writeName(name);
        writeDBPointer(value);
    }

    @Override
    public void writeDBPointer(final BsonDbPointer value) {
        notNull("value", value);
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
    public void writeDecimal128(final Decimal128 value) {
        notNull("value", value);
        checkPreconditions("writeInt64", State.VALUE);
        doWriteDecimal128(value);
        setState(getNextState());
    }

    @Override
    public void writeDecimal128(final String name, final Decimal128 value) {
        notNull("name", name);
        notNull("value", value);
        writeName(name);
        writeDecimal128(value);
    }

    @Override
    public void writeJavaScript(final String name, final String code) {
        notNull("name", name);
        notNull("value", code);
        writeName(name);
        writeJavaScript(code);
    }

    @Override
    public void writeJavaScript(final String code) {
        notNull("value", code);
        checkPreconditions("writeJavaScript", State.VALUE);
        doWriteJavaScript(code);
        setState(getNextState());
    }

    @Override
    public void writeJavaScriptWithScope(final String name, final String code) {
        notNull("name", name);
        notNull("value", code);
        writeName(name);
        writeJavaScriptWithScope(code);
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        notNull("value", code);
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
        notNull("name", name);
        if (state != State.NAME) {
            throwInvalidState("WriteName", State.NAME);
        }
        if (!fieldNameValidatorStack.peek().validate(name)) {
            throw new IllegalArgumentException(format("Invalid BSON field name %s", name));
        }
        doWriteName(name);
        context.name = name;
        state = State.VALUE;
    }

    /**
     * Handles the logic of writing the element name.
     *
     * @param name the name of the element
     * @since 3.5
     */
    protected void doWriteName(final String name) {
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
        notNull("name", name);
        notNull("value", objectId);
        writeName(name);
        writeObjectId(objectId);
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        notNull("value", objectId);
        checkPreconditions("writeObjectId", State.VALUE);
        doWriteObjectId(objectId);
        setState(getNextState());
    }

    @Override
    public void writeRegularExpression(final String name, final BsonRegularExpression regularExpression) {
        notNull("name", name);
        notNull("value", regularExpression);
        writeName(name);
        writeRegularExpression(regularExpression);
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        notNull("value", regularExpression);
        checkPreconditions("writeRegularExpression", State.VALUE);
        doWriteRegularExpression(regularExpression);
        setState(getNextState());
    }

    @Override
    public void writeString(final String name, final String value) {
        notNull("name", name);
        notNull("value", value);
        writeName(name);
        writeString(value);
    }

    @Override
    public void writeString(final String value) {
        notNull("value", value);
        checkPreconditions("writeString", State.VALUE);
        doWriteString(value);
        setState(getNextState());

    }

    @Override
    public void writeSymbol(final String name, final String value) {
        notNull("name", name);
        notNull("value", value);
        writeName(name);
        writeSymbol(value);
    }

    @Override
    public void writeSymbol(final String value) {
        notNull("value", value);
        checkPreconditions("writeSymbol", State.VALUE);
        doWriteSymbol(value);
        setState(getNextState());
    }

    @Override
    public void writeTimestamp(final String name, final BsonTimestamp value) {
        notNull("name", name);
        notNull("value", value);
        writeName(name);
        writeTimestamp(value);
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        notNull("value", value);
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

    /**
     * Returns the next valid state for this writer.  For example, transitions from {@link State#VALUE} to {@link State#NAME} once a value
     * is written.
     *
     * @return the next {@code State}
     */
    protected State getNextState() {
        if (getContext().getContextType() == BsonContextType.ARRAY) {
            return State.VALUE;
        } else {
            return State.NAME;
        }
    }

    /**
     * Checks if this writer's current state is in the list of given states.
     *
     * @param validStates an array of {@code State}s to compare this writer's state to.
     * @return true if this writer's state is in the given list.
     */
    protected boolean checkState(final State[] validStates) {
        for (final State cur : validStates) {
            if (cur == getState()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks the writer is in the correct state. If the writer's current state is in the list of given states, this method will complete
     * without exception.  Throws an {@link java.lang.IllegalStateException} if the writer is closed.  Throws BsonInvalidOperationException
     * if the method is trying to do something that is not permitted in the current state.
     *
     * @param methodName  the name of the method being performed that checks are being performed for
     * @param validStates the list of valid states for this operation
     * @see #throwInvalidState(String, org.bson.AbstractBsonWriter.State...)
     */
    protected void checkPreconditions(final String methodName, final State... validStates) {
        if (isClosed()) {
            throw new IllegalStateException("BsonWriter is closed");
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
     * @throws BsonInvalidOperationException when the method called is not valid for the current ContextType.
     */
    protected void throwInvalidContextType(final String methodName, final BsonContextType actualContextType,
                                           final BsonContextType... validContextTypes) {
        String validContextTypesString = StringUtils.join(" or ", Arrays.asList(validContextTypes));
        throw new BsonInvalidOperationException(format("%s can only be called when ContextType is %s, "
                                                       + "not when ContextType is %s.",
                                                       methodName, validContextTypesString, actualContextType));
    }

    /**
     * Throws a {@link BsonInvalidOperationException} when the method called is not valid for the current state.
     *
     * @param methodName  The name of the method.
     * @param validStates The valid states.
     * @throws BsonInvalidOperationException when the method called is not valid for the current state.
     */
    protected void throwInvalidState(final String methodName, final State... validStates) {
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
                throw new BsonInvalidOperationException(format("%s %s value cannot be written to the root level of a BSON document.",
                                                               article, typeName));
            }
        }

        String validStatesString = StringUtils.join(" or ", Arrays.asList(validStates));
        throw new BsonInvalidOperationException(format("%s can only be called when State is %s, not when State is %s",
                                                       methodName, validStatesString, state));
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void pipe(final BsonReader reader) {
        notNull("reader", reader);
        pipeDocument(reader, null);
    }

    /**
     * Reads a single document from the given BsonReader and writes it to this, appending the given extra elements to the document.
     *
     * @param reader the source of the document
     * @param extraElements the extra elements to append to the document
     * @since 3.6
     */
    public void pipe(final BsonReader reader, final List<BsonElement> extraElements) {
        notNull("reader", reader);
        notNull("extraElements", extraElements);
        pipeDocument(reader, extraElements);
    }

    /**
     * Pipe a list of extra element to this writer
     *
     * @param extraElements the extra elements
     */
    protected void pipeExtraElements(final List<BsonElement> extraElements) {
        notNull("extraElements", extraElements);
        for (BsonElement cur : extraElements) {
            writeName(cur.getName());
            pipeValue(cur.getValue());
        }
    }

    /**
     * Return true if the current execution of the pipe method should be aborted.
     *
     * @return true if the current execution of the pipe method should be aborted.
     *
     * @since 3.7
     */
    protected boolean abortPipe() {
        return false;
    }

    private void pipeDocument(final BsonReader reader, final List<BsonElement> extraElements) {
        reader.readStartDocument();
        writeStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            writeName(reader.readName());
            pipeValue(reader);
            if (abortPipe()) {
                return;
            }
        }
        reader.readEndDocument();
        if (extraElements != null) {
            pipeExtraElements(extraElements);
        }
        writeEndDocument();
    }

    private void pipeJavascriptWithScope(final BsonReader reader) {
        writeJavaScriptWithScope(reader.readJavaScriptWithScope());
        pipeDocument(reader, null);
    }

    private void pipeValue(final BsonReader reader) {
        switch (reader.getCurrentBsonType()) {
            case DOCUMENT:
                pipeDocument(reader, null);
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
            case DECIMAL128:
                writeDecimal128(reader.readDecimal128());
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

    private void pipeDocument(final BsonDocument value) {
        writeStartDocument();
        for (Map.Entry<String, BsonValue> cur : value.entrySet()) {
            writeName(cur.getKey());
            pipeValue(cur.getValue());
        }
        writeEndDocument();
    }

    private void pipeArray(final BsonReader reader) {
        reader.readStartArray();
        writeStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            pipeValue(reader);
            if (abortPipe()) {
                return;
            }
        }
        reader.readEndArray();
        writeEndArray();
    }

    private void pipeArray(final BsonArray array) {
        writeStartArray();
        for (BsonValue cur : array) {
            pipeValue(cur);
        }
        writeEndArray();
    }

    private void pipeJavascriptWithScope(final BsonJavaScriptWithScope javaScriptWithScope) {
        writeJavaScriptWithScope(javaScriptWithScope.getCode());
        pipeDocument(javaScriptWithScope.getScope());
    }

    private void pipeValue(final BsonValue value) {
        switch (value.getBsonType()) {
            case DOCUMENT:
                pipeDocument(value.asDocument());
                break;
            case ARRAY:
                pipeArray(value.asArray());
                break;
            case DOUBLE:
                writeDouble(value.asDouble().getValue());
                break;
            case STRING:
                writeString(value.asString().getValue());
                break;
            case BINARY:
                writeBinaryData(value.asBinary());
                break;
            case UNDEFINED:
                writeUndefined();
                break;
            case OBJECT_ID:
                writeObjectId(value.asObjectId().getValue());
                break;
            case BOOLEAN:
                writeBoolean(value.asBoolean().getValue());
                break;
            case DATE_TIME:
                writeDateTime(value.asDateTime().getValue());
                break;
            case NULL:
                writeNull();
                break;
            case REGULAR_EXPRESSION:
                writeRegularExpression(value.asRegularExpression());
                break;
            case JAVASCRIPT:
                writeJavaScript(value.asJavaScript().getCode());
                break;
            case SYMBOL:
                writeSymbol(value.asSymbol().getSymbol());
                break;
            case JAVASCRIPT_WITH_SCOPE:
                pipeJavascriptWithScope(value.asJavaScriptWithScope());
                break;
            case INT32:
                writeInt32(value.asInt32().getValue());
                break;
            case TIMESTAMP:
                writeTimestamp(value.asTimestamp());
                break;
            case INT64:
                writeInt64(value.asInt64().getValue());
                break;
            case DECIMAL128:
                writeDecimal128(value.asDecimal128().getValue());
                break;
            case MIN_KEY:
                writeMinKey();
                break;
            case DB_POINTER:
                writeDBPointer(value.asDBPointer());
                break;
            case MAX_KEY:
                writeMaxKey();
                break;
            default:
                throw new IllegalArgumentException("unhandled BSON type: " + value.getBsonType());
        }
    }

    /**
     * The state of a writer.  Indicates where in a document the writer is.
     */
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

    /**
     * The context for the writer. Records the parent context, creating a bread crumb trail to trace back up to the root context of the
     * reader. Also records the {@link org.bson.BsonContextType}, indicating whether the writer is reading a document, array, or other
     * complex sub-structure.
     */
    public class Context {
        private final Context parentContext;
        private final BsonContextType contextType;
        private String name;

        /**
         * Creates a new instance, copying values from an existing context.
         *
         * @param from the {@code Context} to copy values from
         */
        public Context(final Context from) {
            parentContext = from.parentContext;
            contextType = from.contextType;
        }

        /**
         * Creates a new instance.
         *
         * @param parentContext the context of the parent node
         * @param contextType   the context type.
         */
        public Context(final Context parentContext, final BsonContextType contextType) {
            this.parentContext = parentContext;
            this.contextType = contextType;
        }

        /**
         * Returns the parent context.  Allows users of this context object to transition to this parent context.
         *
         * @return the context that came before this one
         */
        public Context getParentContext() {
            return parentContext;
        }

        /**
         * Gets the current context type.
         *
         * @return the current context type.
         */
        public BsonContextType getContextType() {
            return contextType;
        }

        /**
         * Copies the values from this {@code Context} into a new instance.
         *
         * @return the new instance with the same values as this context.
         */
        public Context copy() {
            return new Context(this);
        }
    }

    /**
     * Capture the current state of this writer - its {@link org.bson.AbstractBsonWriter.Context}, {@link
     * org.bson.AbstractBsonWriter.State}, field name and depth.
     */
    protected class Mark {
        private final Context markedContext;
        private final State markedState;
        private final String currentName;
        private final int serializationDepth;

        /**
         * Creates a new snapshopt of the current state.
         */
        protected Mark() {
            this.markedContext = AbstractBsonWriter.this.context.copy();
            this.markedState = AbstractBsonWriter.this.state;
            this.currentName = AbstractBsonWriter.this.context.name;
            this.serializationDepth = AbstractBsonWriter.this.serializationDepth;
        }

        /**
         * Resets the {@code AbstractBsonWriter} instance that contains this {@code Mark} to the state the writer was in when the Mark was
         * created.
         */
        protected void reset() {
            setContext(markedContext);
            setState(markedState);
            AbstractBsonWriter.this.context.name = currentName;
            AbstractBsonWriter.this.serializationDepth = serializationDepth;
        }
    }
}
