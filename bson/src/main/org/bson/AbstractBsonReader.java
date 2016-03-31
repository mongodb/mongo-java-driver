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

import java.io.Closeable;

import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Abstract base class for BsonReader implementations.
 *
 * @since 3.0
 */
public abstract class AbstractBsonReader implements Closeable, BsonReader {
    private State state;
    private Context context;
    private BsonType currentBsonType;
    private String currentName;
    private boolean closed;

    /**
     * Initializes a new instance of the BsonReader class.
     */
    protected AbstractBsonReader() {
        state = State.INITIAL;
    }

    @Override
    public BsonType getCurrentBsonType() {
        return currentBsonType;
    }

    @Override
    public String getCurrentName() {
        if (state != State.VALUE) {
            throwInvalidState("getCurrentName", State.VALUE);
        }
        return currentName;
    }

    /**
     * Sets the type of the current value being read.
     *
     * @param newType the BSON Type.
     */
    protected void setCurrentBsonType(final BsonType newType) {
        currentBsonType = newType;
    }

    /**
     * @return The current state of the reader.
     */
    public State getState() {
        return state;
    }

    /**
     * Sets the new current state of this reader.
     *
     * @param newState the state to set this reader to.
     */
    protected void setState(final State newState) {
        state = newState;
    }

    /**
     * Sets the field name for the key/value pair being read.
     *
     * @param newName the field name
     */
    protected void setCurrentName(final String newName) {
        currentName = newName;
    }

    /**
     * Closes the reader.
     */
    public void close() {
        closed = true;
    }

    /**
     * Return true if the reader has been closed.
     *
     * @return true if closed
     */
    protected boolean isClosed() {
        return closed;
    }

    /**
     * Handles the logic to read binary data
     *
     * @return the BsonBinary value
     */
    protected abstract BsonBinary doReadBinaryData();

    /**
     * Handles the logic to peek at the binary subtype.
     *
     * @return the binary subtype
     */
    protected abstract byte doPeekBinarySubType();

    /**
     * Handles the logic to read booleans
     *
     * @return the boolean value
     */
    protected abstract boolean doReadBoolean();

    /**
     * Handles the logic to read date time
     *
     * @return the long value
     */
    protected abstract long doReadDateTime();

    /**
     * Handles the logic to read doubles
     *
     * @return the double value
     */
    protected abstract double doReadDouble();

    /**
     * Handles the logic when reading the end of an array
     */
    protected abstract void doReadEndArray();

    /**
     * Handles the logic when reading the end of a document
     */
    protected abstract void doReadEndDocument();

    /**
     * Handles the logic to read 32 bit ints
     *
     * @return the int value
     */
    protected abstract int doReadInt32();

    /**
     * Handles the logic to read 64 bit ints
     *
     * @return the long value
     */
    protected abstract long doReadInt64();


    /**
     * Handles the logic to read Decimal128
     *
     * @return the Decimal128 value
     * @since 3.4
     */
    protected abstract Decimal128 doReadDecimal128();

    /**
     * Handles the logic to read Javascript functions
     *
     * @return the String value
     */
    protected abstract String doReadJavaScript();

    /**
     * Handles the logic to read scoped Javascript functions
     *
     * @return the String value
     */
    protected abstract String doReadJavaScriptWithScope();

    /**
     * Handles the logic to read a Max key
     */
    protected abstract void doReadMaxKey();

    /**
     * Handles the logic to read a Min key
     */
    protected abstract void doReadMinKey();

    /**
     * Handles the logic to read a null value
     */
    protected abstract void doReadNull();

    /**
     * Handles the logic to read an ObjectId
     *
     * @return the ObjectId value
     */
    protected abstract ObjectId doReadObjectId();

    /**
     * Handles the logic to read a regular expression
     *
     * @return the BsonRegularExpression value
     */
    protected abstract BsonRegularExpression doReadRegularExpression();

    /**
     * Handles the logic to read a DBPointer
     *
     * @return the BsonDbPointer value
     */
    protected abstract BsonDbPointer doReadDBPointer();

    /**
     * Handles the logic to read the start of an array
     */
    protected abstract void doReadStartArray();

    /**
     * Handles the logic to read the start of a document
     */
    protected abstract void doReadStartDocument();

    /**
     * Handles the logic to read a String
     *
     * @return the String value
     */
    protected abstract String doReadString();

    /**
     * Handles the logic to read a Symbol
     *
     * @return the String value
     */
    protected abstract String doReadSymbol();

    /**
     * Handles the logic to read a timestamp
     *
     * @return the BsonTimestamp value
     */
    protected abstract BsonTimestamp doReadTimestamp();

    /**
     * Handles the logic to read an Undefined value
     */
    protected abstract void doReadUndefined();

    /**
     * Handles any logic required to skip the name (reader must be positioned on a name).
     */
    protected abstract void doSkipName();

    /**
     * Handles any logic required to skip the value (reader must be positioned on a value).
     */
    protected abstract void doSkipValue();

    @Override
    public BsonBinary readBinaryData() {
        checkPreconditions("readBinaryData", BsonType.BINARY);
        setState(getNextState());
        return doReadBinaryData();
    }

    @Override
    public byte peekBinarySubType() {
        checkPreconditions("readBinaryData", BsonType.BINARY);
        return doPeekBinarySubType();
    }

    @Override
    public boolean readBoolean() {
        checkPreconditions("readBoolean", BsonType.BOOLEAN);
        setState(getNextState());
        return doReadBoolean();
    }

    @Override
    public abstract BsonType readBsonType();

    @Override
    public long readDateTime() {
        checkPreconditions("readDateTime", BsonType.DATE_TIME);
        setState(getNextState());
        return doReadDateTime();
    }

    @Override
    public double readDouble() {
        checkPreconditions("readDouble", BsonType.DOUBLE);
        setState(getNextState());
        return doReadDouble();
    }

    @Override
    public void readEndArray() {
        if (isClosed()) {
            throw new IllegalStateException("BSONBinaryWriter");
        }
        if (getContext().getContextType() != BsonContextType.ARRAY) {
            throwInvalidContextType("readEndArray", getContext().getContextType(), BsonContextType.ARRAY);
        }
        if (getState() == State.TYPE) {
            readBsonType(); // will set state to EndOfArray if at end of array
        }
        if (getState() != State.END_OF_ARRAY) {
            throwInvalidState("ReadEndArray", State.END_OF_ARRAY);
        }

        doReadEndArray();

        setStateOnEnd();
    }

    @Override
    public void readEndDocument() {
        if (isClosed()) {
            throw new IllegalStateException("BSONBinaryWriter");
        }
        if (getContext().getContextType() != BsonContextType.DOCUMENT && getContext().getContextType() != BsonContextType.SCOPE_DOCUMENT) {
            throwInvalidContextType("readEndDocument",
                                    getContext().getContextType(), BsonContextType.DOCUMENT, BsonContextType.SCOPE_DOCUMENT);
        }
        if (getState() == State.TYPE) {
            readBsonType(); // will set state to EndOfDocument if at end of document
        }
        if (getState() != State.END_OF_DOCUMENT) {
            throwInvalidState("readEndDocument", State.END_OF_DOCUMENT);
        }

        doReadEndDocument();

        setStateOnEnd();
    }

    @Override
    public int readInt32() {
        checkPreconditions("readInt32", BsonType.INT32);
        setState(getNextState());
        return doReadInt32();

    }

    @Override
    public long readInt64() {
        checkPreconditions("readInt64", BsonType.INT64);
        setState(getNextState());
        return doReadInt64();
    }

    @Override
    public Decimal128 readDecimal128() {
        checkPreconditions("readDecimal", BsonType.DECIMAL128);
        setState(getNextState());
        return doReadDecimal128();
    }

    @Override
    public String readJavaScript() {
        checkPreconditions("readJavaScript", BsonType.JAVASCRIPT);
        setState(getNextState());
        return doReadJavaScript();
    }

    @Override
    public String readJavaScriptWithScope() {
        checkPreconditions("readJavaScriptWithScope", BsonType.JAVASCRIPT_WITH_SCOPE);
        setState(State.SCOPE_DOCUMENT);
        return doReadJavaScriptWithScope();
    }

    @Override
    public void readMaxKey() {
        checkPreconditions("readMaxKey", BsonType.MAX_KEY);
        setState(getNextState());
        doReadMaxKey();
    }

    @Override
    public void readMinKey() {
        checkPreconditions("readMinKey", BsonType.MIN_KEY);
        setState(getNextState());
        doReadMinKey();
    }

    @Override
    public void readNull() {
        checkPreconditions("readNull", BsonType.NULL);
        setState(getNextState());
        doReadNull();
    }

    @Override
    public ObjectId readObjectId() {
        checkPreconditions("readObjectId", BsonType.OBJECT_ID);
        setState(getNextState());
        return doReadObjectId();
    }

    @Override
    public BsonRegularExpression readRegularExpression() {
        checkPreconditions("readRegularExpression", BsonType.REGULAR_EXPRESSION);
        setState(getNextState());
        return doReadRegularExpression();
    }

    @Override
    public BsonDbPointer readDBPointer() {
        checkPreconditions("readDBPointer", BsonType.DB_POINTER);
        setState(getNextState());
        return doReadDBPointer();
    }

    @Override
    public void readStartArray() {
        checkPreconditions("readStartArray", BsonType.ARRAY);
        doReadStartArray();
        setState(State.TYPE);
    }

    @Override
    public void readStartDocument() {
        checkPreconditions("readStartDocument", BsonType.DOCUMENT);
        doReadStartDocument();
        setState(State.TYPE);
    }

    @Override
    public String readString() {
        checkPreconditions("readString", BsonType.STRING);
        setState(getNextState());
        return doReadString();
    }

    @Override
    public String readSymbol() {
        checkPreconditions("readSymbol", BsonType.SYMBOL);
        setState(getNextState());
        return doReadSymbol();
    }

    @Override
    public BsonTimestamp readTimestamp() {
        checkPreconditions("readTimestamp", BsonType.TIMESTAMP);
        setState(getNextState());
        return doReadTimestamp();
    }

    @Override
    public void readUndefined() {
        checkPreconditions("readUndefined", BsonType.UNDEFINED);
        setState(getNextState());
        doReadUndefined();
    }

    @Override
    public void skipName() {
        if (isClosed()) {
            throw new IllegalStateException("This instance has been closed");
        }
        if (getState() != State.NAME) {
            throwInvalidState("skipName", State.NAME);
        }
        setState(State.VALUE);
        doSkipName();
    }

    @Override
    public void skipValue() {
        if (isClosed()) {
            throw new IllegalStateException("BSONBinaryWriter");
        }
        if (getState() != State.VALUE) {
            throwInvalidState("skipValue", State.VALUE);
        }

        doSkipValue();

        setState(State.TYPE);
    }

    @Override
    public BsonBinary readBinaryData(final String name) {
        verifyName(name);
        return readBinaryData();
    }

    @Override
    public boolean readBoolean(final String name) {
        verifyName(name);
        return readBoolean();
    }

    @Override
    public long readDateTime(final String name) {
        verifyName(name);
        return readDateTime();
    }

    @Override
    public double readDouble(final String name) {
        verifyName(name);
        return readDouble();
    }

    @Override
    public int readInt32(final String name) {
        verifyName(name);
        return readInt32();
    }

    @Override
    public long readInt64(final String name) {
        verifyName(name);
        return readInt64();
    }

    @Override
    public Decimal128 readDecimal128(final String name) {
        verifyName(name);
        return readDecimal128();
    }

    @Override
    public String readJavaScript(final String name) {
        verifyName(name);
        return readJavaScript();
    }

    @Override
    public String readJavaScriptWithScope(final String name) {
        verifyName(name);
        return readJavaScriptWithScope();
    }

    @Override
    public void readMaxKey(final String name) {
        verifyName(name);
        readMaxKey();
    }

    @Override
    public void readMinKey(final String name) {
        verifyName(name);
        readMinKey();
    }

    @Override
    public String readName() {
        if (state == State.TYPE) {
            readBsonType();
        }
        if (state != State.NAME) {
            throwInvalidState("readName", State.NAME);
        }

        state = State.VALUE;
        return currentName;
    }

    @Override
    public void readName(final String name) {
        verifyName(name);
    }

    @Override
    public void readNull(final String name) {
        verifyName(name);
        readNull();
    }

    @Override
    public ObjectId readObjectId(final String name) {
        verifyName(name);
        return readObjectId();
    }

    @Override
    public BsonRegularExpression readRegularExpression(final String name) {
        verifyName(name);
        return readRegularExpression();
    }

    @Override
    public BsonDbPointer readDBPointer(final String name) {
        verifyName(name);
        return readDBPointer();
    }


    @Override
    public String readString(final String name) {
        verifyName(name);
        return readString();
    }

    @Override
    public String readSymbol(final String name) {
        verifyName(name);
        return readSymbol();
    }

    @Override
    public BsonTimestamp readTimestamp(final String name) {
        verifyName(name);
        return readTimestamp();
    }

    @Override
    public void readUndefined(final String name) {
        verifyName(name);
        readUndefined();
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
        String validContextTypesString = StringUtils.join(" or ", asList(validContextTypes));
        String message = format("%s can only be called when ContextType is %s, not when ContextType is %s.",
                                methodName, validContextTypesString, actualContextType);
        throw new BsonInvalidOperationException(message);
    }

    /**
     * Throws an InvalidOperationException when the method called is not valid for the current state.
     *
     * @param methodName  The name of the method.
     * @param validStates The valid states.
     * @throws BsonInvalidOperationException when the method called is not valid for the current state.
     */
    protected void throwInvalidState(final String methodName, final State... validStates) {
        String validStatesString = StringUtils.join(" or ", asList(validStates));
        String message = format("%s can only be called when State is %s, not when State is %s.",
                                methodName, validStatesString, state);
        throw new BsonInvalidOperationException(message);
    }

    /**
     * Verifies the current state and BSONType of the reader.
     *
     * @param methodName       The name of the method calling this one.
     * @param requiredBsonType The required BSON type.
     */
    protected void verifyBSONType(final String methodName, final BsonType requiredBsonType) {
        if (state == State.INITIAL || state == State.SCOPE_DOCUMENT || state == State.TYPE) {
            readBsonType();
        }
        if (state == State.NAME) {
            // ignore name
            skipName();
        }
        if (state != State.VALUE) {
            throwInvalidState(methodName, State.VALUE);
        }
        if (currentBsonType != requiredBsonType) {
            throw new BsonInvalidOperationException(format("%s can only be called when CurrentBSONType is %s, "
                                                           + "not when CurrentBSONType is %s.",
                                                           methodName, requiredBsonType, currentBsonType));
        }
    }

    /**
     * Verifies the name of the current element.
     *
     * @param expectedName The expected name.
     * @throws BsonSerializationException when the name read is not the expected name
     */
    protected void verifyName(final String expectedName) {
        readBsonType();
        String actualName = readName();
        if (!actualName.equals(expectedName)) {
            throw new BsonSerializationException(format("Expected element name to be '%s', not '%s'.",
                                                        expectedName, actualName));
        }
    }

    /**
     * Ensures any conditions are met before reading commences.  Throws exceptions if the conditions are not met.
     *
     * @param methodName the name of the current method, which will indicate the field being read
     * @param type       the type of this field
     */
    protected void checkPreconditions(final String methodName, final BsonType type) {
        if (isClosed()) {
            throw new IllegalStateException("BsonWriter is closed");
        }

        verifyBSONType(methodName, type);
    }

    /**
     * Get the context, which will indicate which state the reader is in, for example which part of a document it's currently reading.
     *
     * @return the context
     */
    protected Context getContext() {
        return context;
    }

    /**
     * Set the context, which will indicate which state the reader is in, for example which part of a document it's currently reading.
     *
     * @param context the current context.
     */
    protected void setContext(final Context context) {
        this.context = context;
    }

    /**
     * Returns the next {@code State} to transition to, based on the {@link org.bson.AbstractBsonReader.Context} of this reader.
     *
     * @return the next state
     */
    protected State getNextState() {
        switch (context.getContextType()) {
            case ARRAY:
            case DOCUMENT:
            case SCOPE_DOCUMENT:
                return State.TYPE;
            case TOP_LEVEL:
                return State.DONE;
            default:
                throw new BSONException(format("Unexpected ContextType %s.", context.getContextType()));
        }
    }

    private void setStateOnEnd() {
        switch (getContext().getContextType()) {
            case ARRAY:
            case DOCUMENT:
                setState(State.TYPE);
                break;
            case TOP_LEVEL:
                setState(State.DONE);
                break;
            default:
                throw new BSONException(format("Unexpected ContextType %s.", getContext().getContextType()));
        }
    }
    protected class Mark {
        private State state;
        private Context parentContext;
        private BsonContextType contextType;
        private BsonType currentBsonType;
        private String currentName;

        protected Context getParentContext() {
            return parentContext;
        }

        protected BsonContextType getContextType() {
            return contextType;
        }

        protected Mark() {
            state = AbstractBsonReader.this.state;
            parentContext = AbstractBsonReader.this.context.parentContext;
            contextType = AbstractBsonReader.this.context.contextType;
            currentBsonType = AbstractBsonReader.this.currentBsonType;
            currentName = AbstractBsonReader.this.currentName;
        }

        protected void reset() {
            AbstractBsonReader.this.state = state;
            AbstractBsonReader.this.currentBsonType = currentBsonType;
            AbstractBsonReader.this.currentName = currentName;
        }
    }


    /**
     * The context for the reader. Records the parent context, creating a bread crumb trail to trace back up to the root context of the
     * reader. Also records the {@link org.bson.BsonContextType}, indicating whether the reader is reading a document, array, or other
     * complex sub-structure.
     */
    protected abstract class Context {

        private final Context parentContext;
        private final BsonContextType contextType;

        /**
         * Creates a new instance.
         *
         * @param parentContext a possibly null value for the context that came before this one
         * @param contextType   the type of this context
         */
        protected Context(final Context parentContext, final BsonContextType contextType) {
            this.parentContext = parentContext;
            this.contextType = contextType;
        }

        /**
         * Returns the parent context.  Allows users of this context object to transition to this parent context.
         *
         * @return the context that came before this one
         */
        protected Context getParentContext() {
            return parentContext;
        }

        /**
         * Return the type of this context.
         *
         * @return the context type.
         */
        protected BsonContextType getContextType() {
            return contextType;
        }
    }

    /**
     * The state of a reader.  Indicates where in a document the reader is.
     */
    public enum State {
        /**
         * The initial state.
         */
        INITIAL,

        /**
         * The reader is positioned at the type of an element or value.
         */
        TYPE,

        /**
         * The reader is positioned at the name of an element.
         */
        NAME,

        /**
         * The reader is positioned at a value.
         */
        VALUE,

        /**
         * The reader is positioned at a scope document.
         */
        SCOPE_DOCUMENT,

        /**
         * The reader is positioned at the end of a document.
         */
        END_OF_DOCUMENT,

        /**
         * The reader is positioned at the end of an array.
         */
        END_OF_ARRAY,

        /**
         * The reader has finished reading a document.
         */
        DONE,

        /**
         * The reader is closed.
         */
        CLOSED
    }
}
