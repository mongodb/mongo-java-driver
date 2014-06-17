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
import org.bson.types.Undefined;

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
     *
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

    protected void setCurrentBsonType(final BsonType newType) {
        currentBsonType = newType;
    }

    /**
     * @return The current state of the reader.
     */
    public State getState() {
        return state;
    }

    protected void setState(final State newState) {
        state = newState;
    }

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

    protected abstract Binary doReadBinaryData();

    protected abstract boolean doReadBoolean();

    protected abstract long doReadDateTime();

    protected abstract double doReadDouble();

    protected abstract void doReadEndArray();

    protected abstract void doReadEndDocument();

    protected abstract int doReadInt32();

    protected abstract long doReadInt64();

    protected abstract String doReadJavaScript();

    protected abstract String doReadJavaScriptWithScope();

    protected abstract void doReadMaxKey();

    protected abstract void doReadMinKey();

    protected abstract void doReadNull();

    protected abstract ObjectId doReadObjectId();

    protected abstract RegularExpression doReadRegularExpression();

    protected abstract DBPointer doReadDBPointer();

    protected abstract void doReadStartArray();

    protected abstract void doReadStartDocument();

    protected abstract String doReadString();

    protected abstract String doReadSymbol();

    protected abstract Timestamp doReadTimestamp();

    protected abstract Undefined doReadUndefined();

    protected abstract void doSkipName();

    protected abstract void doSkipValue();

    @Override
    public Binary readBinaryData() {
        checkPreconditions("readBinaryData", BsonType.BINARY);
        setState(getNextState());

        return doReadBinaryData();
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
    public RegularExpression readRegularExpression() {
        checkPreconditions("readRegularExpression", BsonType.REGULAR_EXPRESSION);
        setState(getNextState());
        return doReadRegularExpression();
    }

    @Override
    public DBPointer readDBPointer() {
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
    public Timestamp readTimestamp() {
        checkPreconditions("readTimestamp", BsonType.TIMESTAMP);
        setState(getNextState());
        return doReadTimestamp();
    }

    @Override
    public Undefined readUndefined() {
        checkPreconditions("readUndefined", BsonType.UNDEFINED);
        setState(getNextState());
        return doReadUndefined();
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
    public Binary readBinaryData(final String name) {
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
    public RegularExpression readRegularExpression(final String name) {
        verifyName(name);
        return readRegularExpression();
    }

    @Override
    public DBPointer readDBPointer(final String name) {
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
    public Timestamp readTimestamp(final String name) {
        verifyName(name);
        return readTimestamp();
    }

    @Override
    public Undefined readUndefined(final String name) {
        verifyName(name);
        return readUndefined();
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
     * @throws BsonInvalidOperationException
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
            String message = format("%s can only be called when CurrentBSONType is %s, not when CurrentBSONType is %s.",
                                    methodName, requiredBsonType, currentBsonType);
            throw new BsonInvalidOperationException(message);
        }
    }

    /**
     * Verifies the name of the current element.
     *
     * @param expectedName The expected name.
     * @throws BsonSerializationException
     */
    protected void verifyName(final String expectedName) {
        readBsonType();
        String actualName = readName();
        if (!actualName.equals(expectedName)) {
            String message = format("Expected element name to be '%s', not '%s'.",
                                    expectedName, actualName);
            throw new BsonSerializationException(message);
        }
    }

    protected void checkPreconditions(final String methodName, final BsonType type) {
        if (isClosed()) {
            throw new IllegalStateException("BsonWriter is closed");
        }

        verifyBSONType(methodName, type);
    }

    protected Context getContext() {
        return context;
    }

    protected void setContext(final Context context) {
        this.context = context;
    }

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

    protected static class Context {
        private final Context parentContext;
        private final BsonContextType contextType;

        protected Context(final Context parentContext, final BsonContextType contextType) {
            this.parentContext = parentContext;
            this.contextType = contextType;
        }

        protected Context getParentContext() {
            return parentContext;
        }

        protected BsonContextType getContextType() {
            return contextType;
        }
    }

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
