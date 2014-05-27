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

import static java.lang.String.format;
import static java.util.Arrays.asList;

public abstract class AbstractBSONReader implements Closeable, BSONReader {
    private final BSONReaderSettings settings;
    private State state;
    private Context context;
    private BSONType currentBSONType;
    private String currentName;
    private boolean closed;

    /**
     * Initializes a new instance of the BSONReader class.
     *
     * @param settings The reader settings.
     */
    protected AbstractBSONReader(final BSONReaderSettings settings) {
        this.settings = settings;
        state = State.INITIAL;
    }

    @Override
    public BSONType getCurrentBSONType() {
        return currentBSONType;
    }

    @Override
    public String getCurrentName() {
        if (state != State.VALUE) {
            throwInvalidState("getCurrentName", State.VALUE);
        }
        return currentName;
    }

    protected void setCurrentBSONType(final BSONType newType) {
        currentBSONType = newType;
    }

    /**
     * @return The settings of the reader.
     */
    public BSONReaderSettings getSettings() {
        return settings;
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

    public boolean isClosed() {
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

    protected abstract void doReadUndefined();

    protected abstract void doSkipName();

    protected abstract void doSkipValue();

    @Override
    public Binary readBinaryData() {
        checkPreconditions("readBinaryData", BSONType.BINARY);
        setState(getNextState());

        return doReadBinaryData();
    }

    @Override
    public boolean readBoolean() {
        checkPreconditions("readBoolean", BSONType.BOOLEAN);
        setState(getNextState());
        return doReadBoolean();
    }

    @Override
    public abstract BSONType readBSONType();

    @Override
    public long readDateTime() {
        checkPreconditions("readDateTime", BSONType.DATE_TIME);
        setState(getNextState());
        return doReadDateTime();
    }

    @Override
    public double readDouble() {
        checkPreconditions("readDouble", BSONType.DOUBLE);
        setState(getNextState());
        return doReadDouble();
    }

    @Override
    public void readEndArray() {
        if (isClosed()) {
            throw new IllegalStateException("BSONBinaryWriter");
        }
        if (getContext().getContextType() != BSONContextType.ARRAY) {
            throwInvalidContextType("readEndArray", getContext().getContextType(), BSONContextType.ARRAY);
        }
        if (getState() == State.TYPE) {
            readBSONType(); // will set state to EndOfArray if at end of array
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
        if (getContext().getContextType() != BSONContextType.DOCUMENT && getContext().getContextType() != BSONContextType.SCOPE_DOCUMENT) {
            throwInvalidContextType("readEndDocument",
                                    getContext().getContextType(), BSONContextType.DOCUMENT, BSONContextType.SCOPE_DOCUMENT);
        }
        if (getState() == State.TYPE) {
            readBSONType(); // will set state to EndOfDocument if at end of document
        }
        if (getState() != State.END_OF_DOCUMENT) {
            throwInvalidState("readEndDocument", State.END_OF_DOCUMENT);
        }

        doReadEndDocument();

        setStateOnEnd();
    }

    @Override
    public int readInt32() {
        checkPreconditions("readInt32", BSONType.INT32);
        setState(getNextState());
        return doReadInt32();

    }

    @Override
    public long readInt64() {
        checkPreconditions("readInt64", BSONType.INT64);
        setState(getNextState());
        return doReadInt64();
    }

    @Override
    public String readJavaScript() {
        checkPreconditions("readJavaScript", BSONType.JAVASCRIPT);
        setState(getNextState());
        return doReadJavaScript();
    }

    @Override
    public String readJavaScriptWithScope() {
        checkPreconditions("readJavaScriptWithScope", BSONType.JAVASCRIPT_WITH_SCOPE);
        setState(State.SCOPE_DOCUMENT);
        return doReadJavaScriptWithScope();
    }

    @Override
    public void readMaxKey() {
        checkPreconditions("readMaxKey", BSONType.MAX_KEY);
        setState(getNextState());
        doReadMaxKey();
    }

    @Override
    public void readMinKey() {
        checkPreconditions("readMinKey", BSONType.MIN_KEY);
        setState(getNextState());
        doReadMinKey();
    }

    @Override
    public void readNull() {
        checkPreconditions("readNull", BSONType.NULL);
        setState(getNextState());
        doReadNull();
    }

    @Override
    public ObjectId readObjectId() {
        checkPreconditions("readObjectId", BSONType.OBJECT_ID);
        setState(getNextState());
        return doReadObjectId();
    }

    @Override
    public RegularExpression readRegularExpression() {
        checkPreconditions("readRegularExpression", BSONType.REGULAR_EXPRESSION);
        setState(getNextState());
        return doReadRegularExpression();
    }

    @Override
    public DBPointer readDBPointer() {
        checkPreconditions("readDBPointer", BSONType.DB_POINTER);
        setState(getNextState());
        return doReadDBPointer();
    }

    @Override
    public void readStartArray() {
        checkPreconditions("readStartArray", BSONType.ARRAY);
        doReadStartArray();
        setState(State.TYPE);
    }

    @Override
    public void readStartDocument() {
        checkPreconditions("readStartDocument", BSONType.DOCUMENT);
        doReadStartDocument();
        setState(State.TYPE);
    }

    @Override
    public String readString() {
        checkPreconditions("readString", BSONType.STRING);
        setState(getNextState());
        return doReadString();
    }

    @Override
    public String readSymbol() {
        checkPreconditions("readSymbol", BSONType.SYMBOL);
        setState(getNextState());
        return doReadSymbol();
    }

    @Override
    public Timestamp readTimestamp() {
        checkPreconditions("readTimestamp", BSONType.TIMESTAMP);
        setState(getNextState());
        return doReadTimestamp();
    }

    @Override
    public void readUndefined() {
        checkPreconditions("readUndefined", BSONType.UNDEFINED);
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
            readBSONType();
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
     * @throws BSONInvalidOperationException
     */
    protected void throwInvalidContextType(final String methodName, final BSONContextType actualContextType,
                                           final BSONContextType... validContextTypes) {
        String validContextTypesString = StringUtils.join(" or ", asList(validContextTypes));
        String message = format("%s can only be called when ContextType is %s, not when ContextType is %s.",
                                methodName, validContextTypesString, actualContextType);
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
        String validStatesString = StringUtils.join(" or ", asList(validStates));
        String message = format("%s can only be called when State is %s, not when State is %s.",
                                methodName, validStatesString, state);
        throw new BSONInvalidOperationException(message);
    }

    /**
     * Verifies the current state and BSONType of the reader.
     *
     * @param methodName       The name of the method calling this one.
     * @param requiredBSONType The required BSON type.
     */
    protected void verifyBSONType(final String methodName, final BSONType requiredBSONType) {
        if (state == State.INITIAL || state == State.SCOPE_DOCUMENT || state == State.TYPE) {
            readBSONType();
        }
        if (state == State.NAME) {
            // ignore name
            skipName();
        }
        if (state != State.VALUE) {
            throwInvalidState(methodName, State.VALUE);
        }
        if (currentBSONType != requiredBSONType) {
            String message = format("%s can only be called when CurrentBSONType is %s, not when CurrentBSONType is %s.",
                                    methodName, requiredBSONType, currentBSONType);
            throw new BSONInvalidOperationException(message);
        }
    }

    /**
     * Verifies the name of the current element.
     *
     * @param expectedName The expected name.
     * @throws BSONSerializationException
     */
    protected void verifyName(final String expectedName) {
        readBSONType();
        String actualName = readName();
        if (!actualName.equals(expectedName)) {
            String message = format("Expected element name to be '%s', not '%s'.",
                                    expectedName, actualName);
            throw new BSONSerializationException(message);
        }
    }

    protected void checkPreconditions(final String methodName, final BSONType type) {
        if (isClosed()) {
            throw new IllegalStateException("BSONWriter is closed");
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
        private final BSONContextType contextType;

        protected Context(final Context parentContext, final BSONContextType contextType) {
            this.parentContext = parentContext;
            this.contextType = contextType;
        }

        protected Context getParentContext() {
            return parentContext;
        }

        protected BSONContextType getContextType() {
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
