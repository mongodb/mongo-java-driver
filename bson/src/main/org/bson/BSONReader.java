/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;

import java.io.Closeable;
import java.util.Arrays;

public abstract class BSONReader implements Closeable {
    private final BSONReaderSettings settings;
    private State state;
    private BSONType currentBSONType;
    private String currentName;
    private boolean closed;

    /**
     * Initializes a new instance of the BSONReader class.
     *
     * @param settings The reader settings.
     */
    protected BSONReader(final BSONReaderSettings settings) {
        this.settings = settings;
        state = State.INITIAL;
    }

    /**
     * @return The current BSONType.
     */
    public BSONType getCurrentBSONType() {
        return currentBSONType;
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

    /**
     * @return The current name.
     */
    protected String getCurrentName() {
        return currentName;
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

    /**
     * @return The current BSONType (calls readBSONType if necessary).
     */
    public BSONType getNextBSONType() {
        if (state == State.INITIAL || state == State.DONE || state == State.SCOPE_DOCUMENT || state == State.TYPE) {
            readBSONType();
        }
        if (state != State.VALUE) {
            throwInvalidState("GetCurrentBSONType", State.VALUE);
        }
        return currentBSONType;
    }

    /**
     * Reads BSON Binary data from the reader.
     *
     * @return A Binary.
     */
    public abstract Binary readBinaryData();

    /**
     * Reads a BSON Binary data element from the reader.
     *
     * @param name The name of the element.
     * @return A Binary.
     */
    public Binary readBinaryData(final String name) {
        verifyName(name);
        return readBinaryData();
    }

    /**
     * Reads a BSON Boolean from the reader.
     *
     * @return A Boolean.
     */
    public abstract boolean readBoolean();

    /**
     * Reads a BSON Boolean element from the reader.
     *
     * @param name The name of the element.
     * @return A Boolean.
     */
    public boolean readBoolean(final String name) {
        verifyName(name);
        return readBoolean();
    }

    /**
     * Reads a BSONType from the reader.
     *
     * @return A BSONType.
     */
    public abstract BSONType readBSONType();

    /**
     * Reads a BSON DateTime from the reader.
     *
     * @return The number of milliseconds since the Unix epoch.
     */
    public abstract long readDateTime();

    /**
     * Reads a BSON DateTime element from the reader.
     *
     * @param name The name of the element.
     * @return The number of milliseconds since the Unix epoch.
     */
    public long readDateTime(final String name) {
        verifyName(name);
        return readDateTime();
    }

    /**
     * Reads a BSON Double from the reader.
     *
     * @return A Double.
     */
    public abstract double readDouble();

    /**
     * Reads a BSON Double element from the reader.
     *
     * @param name The name of the element.
     * @return A Double.
     */
    public double readDouble(final String name) {
        verifyName(name);
        return readDouble();
    }

    /**
     * Reads the end of a BSON array from the reader.
     */
    public abstract void readEndArray();

    /**
     * Reads the end of a BSON document from the reader.
     */
    public abstract void readEndDocument();

    /**
     * Reads a BSON Int32 from the reader.
     *
     * @return An Int32.
     */
    public abstract int readInt32();

    /**
     * Reads a BSON Int32 element from the reader.
     *
     * @param name The name of the element.
     * @return An Int32.
     */
    public int readInt32(final String name) {
        verifyName(name);
        return readInt32();
    }

    /**
     * Reads a BSON Int64 from the reader.
     *
     * @return An Int64.
     */
    public abstract long readInt64();

    /**
     * Reads a BSON Int64 element from the reader.
     *
     * @param name The name of the element.
     * @return An Int64.
     */
    public long readInt64(final String name) {
        verifyName(name);
        return readInt64();
    }

    /**
     * Reads a BSON JavaScript from the reader.
     *
     * @return A string.
     */
    public abstract String readJavaScript();

    /**
     * Reads a BSON JavaScript element from the reader.
     *
     * @param name The name of the element.
     * @return A string.
     */
    public String readJavaScript(final String name) {
        verifyName(name);
        return readJavaScript();
    }

    /**
     * Reads a BSON JavaScript with scope from the reader (call readStartDocument next to read the scope).
     *
     * @return A string.
     */
    public abstract String readJavaScriptWithScope();

    /**
     * Reads a BSON JavaScript with scope element from the reader (call readStartDocument next to read the scope).
     *
     * @param name The name of the element.
     * @return A string.
     */
    public String readJavaScriptWithScope(final String name) {
        verifyName(name);
        return readJavaScriptWithScope();
    }

    /**
     * Reads a BSON MaxKey from the reader.
     */
    public abstract void readMaxKey();

    /**
     * Reads a BSON MaxKey element from the reader.
     *
     * @param name The name of the element.
     */
    public void readMaxKey(final String name) {
        verifyName(name);
        readMaxKey();
    }

    /**
     * Reads a BSON MinKey from the reader.
     */
    public abstract void readMinKey();

    /**
     * Reads a BSON MinKey element from the reader.
     *
     * @param name The name of the element.
     */
    public void readMinKey(final String name) {
        verifyName(name);
        readMinKey();
    }

    /**
     * Reads the name of an element from the reader.
     *
     * @return The name of the element.
     */
    public String readName() {
        if (state == State.TYPE) {
            readBSONType();
        }
        if (state != State.NAME) {
            throwInvalidState("ReadName", State.NAME);
        }

        state = State.VALUE;
        return currentName;
    }

    /**
     * Reads the name of an element from the reader.
     *
     * @param name The name of the element.
     */
    public void readName(final String name) {
        verifyName(name);
    }

    /**
     * Reads a BSON null from the reader.
     */
    public abstract void readNull();

    /**
     * Reads a BSON null element from the reader.
     *
     * @param name The name of the element.
     */
    public void readNull(final String name) {
        verifyName(name);
        readNull();
    }

    /**
     * Reads a BSON ObjectId from the reader.
     */
    public abstract ObjectId readObjectId();

    /**
     * Reads a BSON ObjectId element from the reader.
     *
     * @param name The name of the element.
     * @return ObjectId.
     */
    public ObjectId readObjectId(final String name) {
        verifyName(name);
        return readObjectId();
    }

    /**
     * Reads a BSON regular expression from the reader.
     *
     * @return A regular expression.
     */
    public abstract RegularExpression readRegularExpression();

    /**
     * Reads a BSON regular expression element from the reader.
     *
     * @param name The name of the element.
     * @return A regular expression.
     */
    public RegularExpression readRegularExpression(final String name) {
        verifyName(name);
        return readRegularExpression();
    }

    /**
     * Reads the start of a BSON array.
     */
    public abstract void readStartArray();

    /**
     * Reads the start of a BSON document.
     */
    public abstract void readStartDocument();

    /**
     * Reads a BSON String from the reader.
     *
     * @return A String.
     */
    public abstract String readString();

    /**
     * Reads a BSON string element from the reader.
     *
     * @param name The name of the element.
     * @return A String.
     */
    public String readString(final String name) {
        verifyName(name);
        return readString();
    }

    /**
     * Reads a BSON symbol from the reader.
     *
     * @return A string.
     */
    public abstract String readSymbol();

    /**
     * Reads a BSON symbol element from the reader.
     *
     * @param name The name of the element.
     * @return A string.
     */
    public String readSymbol(final String name) {
        verifyName(name);
        return readSymbol();
    }

    /**
     * Reads a BSON timestamp from the reader.
     *
     * @return The combined timestamp/increment.
     */
    public abstract BSONTimestamp readTimestamp();

    /**
     * Reads a BSON timestamp element from the reader.
     *
     * @param name The name of the element.
     * @return The combined timestamp/increment.
     */
    public BSONTimestamp readTimestamp(final String name) {
        verifyName(name);
        return readTimestamp();
    }

    /**
     * Reads a BSON undefined from the reader.
     */
    public abstract void readUndefined();

    /**
     * Reads a BSON undefined element from the reader.
     *
     * @param name The name of the element.
     */
    public void readUndefined(final String name) {
        verifyName(name);
        readUndefined();
    }

    /**
     * Skips the name (reader must be positioned on a name).
     */
    public abstract void skipName();

    /**
     * Skips the value (reader must be positioned on a value).
     */
    public abstract void skipValue();

    /**
     * Throws an InvalidOperationException when the method called is not valid for the current ContextType.
     *
     * @param methodName        The name of the method.
     * @param actualContextType The actual ContextType.
     * @param validContextTypes The valid ContextTypes.
     *
     * @throws BSONInvalidOperationException
     */
    protected void throwInvalidContextType(final String methodName, final BSONContextType actualContextType,
                                           final BSONContextType... validContextTypes) {
        final String validContextTypesString = StringUtils.join(" or ", Arrays.asList(validContextTypes));
        final String message = String.format(
                "%s can only be called when ContextType is %s, not when ContextType is %s.",
                methodName, validContextTypesString, actualContextType);
        throw new BSONInvalidOperationException(message);
    }

    /**
     * Throws an InvalidOperationException when the method called is not valid for the current state.
     *
     * @param methodName  The name of the method.
     * @param validStates The valid states.
     *
     * @throws BSONInvalidOperationException
     */
    protected void throwInvalidState(final String methodName, final State... validStates) {
        final String validStatesString = StringUtils.join(" or ", Arrays.asList(validStates));
        final String message = String.format(
                "%s can only be called when State is %s, not when State is %s.",
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
            final String message = String.format(
                    "%s can only be called when CurrentBSONType is %s, not when CurrentBSONType is %s.",
                    methodName, requiredBSONType, currentBSONType);
            throw new BSONInvalidOperationException(message);
        }
    }

    /**
     * Verifies the name of the current element.
     *
     * @param expectedName The expected name.
     *
     * @throws BSONSerializationException
     */
    protected void verifyName(final String expectedName) {
        readBSONType();
        final String actualName = readName();
        if (!actualName.equals(expectedName)) {
            final String message = String.format(
                    "Expected element name to be '%s', not '%s'.",
                    expectedName, actualName);
            throw new BSONSerializationException(message);
        }
    }

    protected enum State {
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
