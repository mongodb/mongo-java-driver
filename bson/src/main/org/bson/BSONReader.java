/*
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
 */

package org.bson;

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.bson.util.StringUtils;

import java.io.Closeable;
import java.util.Arrays;

public abstract class BSONReader implements Closeable {
    private final BsonReaderSettings settings;
    private State state;
    private BsonType currentBsonType;
    private String currentName;
    private boolean closed;

    // constructors
    /// <summary>
    /// Initializes a new instance of the BsonReader class.
    /// </summary>
    /// <param name="settings">The reader settings.</param>
    protected BSONReader(final BsonReaderSettings settings) {
        this.settings = settings;
        state = State.INITIAL;
    }

    // public properties
    /// <summary>
    /// Gets the current BsonType.
    /// </summary>
    public BsonType getCurrentBsonType() {
        return currentBsonType;
    }

    protected void setCurrentBsonType(final BsonType newType) {
        currentBsonType = newType;
    }

    /// <summary>
    /// Gets the settings of the reader.
    /// </summary>
    public BsonReaderSettings getSettings() {
        return settings;
    }

    /// <summary>
    /// Gets the current state of the reader.
    /// </summary>
    public State getState() {
        return state;
    }

    protected void setState(final State newState) {
        state = newState;
    }

    // protected properties
    /// <summary>
    /// Gets the current name.
    /// </summary>
    protected String getCurrentName() {
        return currentName;
    }

    protected void setCurrentName(final String newName) {
        currentName = newName;
    }

    // public methods
    /// <summary>
    /// Closes the reader.
    /// </summary>
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    /// <summary>
    /// Positions the reader to an element by name.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <returns>True if the element was found.</returns>
//    public bool FindElement(string name)
//    {
//        if (_disposed) { ThrowObjectDisposedException(); }
//        if (state != State.TYPE)
//        {
//            ThrowInvalidState("FindElement", State.TYPE);
//        }
//
//        BsonType bsonType;
//        while ((bsonType = ReadBsonType()) != BsonType.END_OF_DOCUMENT)
//        {
//            var elementName = ReadName();
//            if (elementName == name)
//            {
//                return true;
//            }
//            SkipValue();
//        }
//
//        return false;
//    }

    /// <summary>
    /// Positions the reader to a string element by name.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <returns>True if the element was found.</returns>
//    public string FindStringElement(string name)
//    {
//        if (_disposed) { ThrowObjectDisposedException(); }
//        if (state != State.TYPE)
//        {
//            ThrowInvalidState("FindStringElement", State.TYPE);
//        }
//
//        BsonType bsonType;
//        while ((bsonType = readBsonType()) != BsonType.END_OF_DOCUMENT)
//        {
//            var elementName = readName();
//            if (bsonType == BsonType.String && elementName == name)
//            {
//                return readString();
//            }
//            else
//            {
//                SkipValue();
//            }
//        }
//
//        return null;
//    }

    /// <summary>
    /// Gets a bookmark to the reader's current position and state.
    /// </summary>
    /// <returns>A bookmark.</returns>
//    public abstract BsonReaderBookmark GetBookmark();

    /// <summary>
    /// Gets the current BsonType (calls readBsonType if necessary).
    /// </summary>
    /// <returns>The current BsonType.</returns>
    public BsonType getNextBsonType() {
        if (state == State.INITIAL || state == State.DONE || state == State.SCOPE_DOCUMENT || state == State.TYPE) {
            readBsonType();
        }
        if (state != State.VALUE) {
            throwInvalidState("GetCurrentBsonType", State.VALUE);
        }
        return currentBsonType;
    }

    /// <summary>
    /// Reads BSON binary data from the reader.
    /// </summary>
    /// <param name="bytes">The binary data.</param>
    /// <param name="subType">The binary data subtype.</param>
    public abstract Binary readBinaryData();

    /// <summary>
    /// Reads a BSON binary data element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="bytes">The binary data.</param>
    /// <param name="subType">The binary data subtype.</param>
    public Binary readBinaryData(final String name) {
        verifyName(name);
        return readBinaryData();
    }

    /// <summary>
    /// Reads a BSON boolean from the reader.
    /// </summary>
    /// <returns>A Boolean.</returns>
    public abstract boolean readBoolean();

    /// <summary>
    /// Reads a BSON boolean element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <returns>A Boolean.</returns>
    public boolean readBoolean(final String name) {
        verifyName(name);
        return readBoolean();
    }

    /// <summary>
    /// Reads a BsonType from the reader.
    /// </summary>
    /// <returns>A BsonType.</returns>
    public abstract BsonType readBsonType();

    /// <summary>
    /// Reads a BSON DateTime from the reader.
    /// </summary>
    /// <returns>The number of milliseconds since the Unix epoch.</returns>
    public abstract long readDateTime();

    /// <summary>
    /// Reads a BSON DateTime element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <returns>The number of milliseconds since the Unix epoch.</returns>
    public long readDateTime(final String name) {
        verifyName(name);
        return readDateTime();
    }

    /// <summary>
    /// Reads a BSON Double from the reader.
    /// </summary>
    /// <returns>A Double.</returns>
    public abstract double readDouble();

    /// <summary>
    /// Reads a BSON Double element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <returns>A Double.</returns>
    public double readDouble(final String name) {
        verifyName(name);
        return readDouble();
    }

    /// <summary>
    /// Reads the end of a BSON array from the reader.
    /// </summary>
    public abstract void readEndArray();

    /// <summary>
    /// Reads the end of a BSON document from the reader.
    /// </summary>
    public abstract void readEndDocument();

    /// <summary>
    /// Reads a BSON INT32 from the reader.
    /// </summary>
    /// <returns>An INT32.</returns>
    public abstract int readInt32();

    /// <summary>
    /// Reads a BSON INT32 element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <returns>An INT32.</returns>
    public int readInt32(final String name) {
        verifyName(name);
        return readInt32();
    }

    /// <summary>
    /// Reads a BSON Int64 from the reader.
    /// </summary>
    /// <returns>An Int64.</returns>
    public abstract long readInt64();

    /// <summary>
    /// Reads a BSON Int64 element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <returns>An Int64.</returns>
    public long readInt64(final String name) {
        verifyName(name);
        return readInt64();
    }

    /// <summary>
    /// Reads a BSON JavaScript from the reader.
    /// </summary>
    /// <returns>A string.</returns>
    public abstract String readJavaScript();

    /// <summary>
    /// Reads a BSON JavaScript element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <returns>A string.</returns>
    public String readJavaScript(final String name) {
        verifyName(name);
        return readJavaScript();
    }

    /// <summary>
    /// Reads a BSON JavaScript with scope from the reader (call readStartDocument next to read the scope).
    /// </summary>
    /// <returns>A string.</returns>
    public abstract String readJavaScriptWithScope();

    /// <summary>
    /// Reads a BSON JavaScript with scope element from the reader (call readStartDocument next to read the scope).
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <returns>A string.</returns>
    public String readJavaScriptWithScope(final String name) {
        verifyName(name);
        return readJavaScriptWithScope();
    }

    /// <summary>
    /// Reads a BSON MaxKey from the reader.
    /// </summary>
    public abstract void readMaxKey();

    /// <summary>
    /// Reads a BSON MaxKey element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void readMaxKey(final String name) {
        verifyName(name);
        readMaxKey();
    }

    /// <summary>
    /// Reads a BSON MinKey from the reader.
    /// </summary>
    public abstract void readMinKey();

    /// <summary>
    /// Reads a BSON MinKey element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void readMinKey(final String name) {
        verifyName(name);
        readMinKey();
    }

    /// <summary>
    /// Reads the name of an element from the reader.
    /// </summary>
    /// <returns>The name of the element.</returns>
    public String readName() {
        if (state == State.TYPE) {
            readBsonType();
        }
        if (state != State.NAME) {
            throwInvalidState("ReadName", State.NAME);
        }

        state = State.VALUE;
        return currentName;
    }

    /// <summary>
    /// Reads the name of an element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void readName(final String name) {
        verifyName(name);
    }

    /// <summary>
    /// Reads a BSON null from the reader.
    /// </summary>
    public abstract void readNull();

    /// <summary>
    /// Reads a BSON null element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void readNull(final String name) {
        verifyName(name);
        readNull();
    }

    /// <summary>
    /// Reads a BSON ObjectId from the reader.
    /// </summary>
    /// <param name="timestamp">The timestamp.</param>
    /// <param name="machine">The machine hash.</param>
    /// <param name="pid">The PID.</param>
    /// <param name="increment">The increment.</param>
    public abstract ObjectId readObjectId();

    /// <summary>
    /// Reads a BSON ObjectId element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <param name="timestamp">The timestamp.</param>
    /// <param name="machine">The machine hash.</param>
    /// <param name="pid">The PID.</param>
    /// <param name="increment">The increment.</param>
    public ObjectId readObjectId(final String name) {
        verifyName(name);
        return readObjectId();
    }

    /// <summary>
    /// Reads a BSON regular expression from the reader.
    /// </summary>
    public abstract RegularExpression readRegularExpression();

    /// <summary>
    /// Reads a BSON regular expression element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public RegularExpression readRegularExpression(final String name) {
        verifyName(name);
        return readRegularExpression();
    }

    /// <summary>
    /// Reads the start of a BSON array.
    /// </summary>
    public abstract void readStartArray();

    /// <summary>
    /// Reads the start of a BSON document.
    /// </summary>
    public abstract void readStartDocument();

    /// <summary>
    /// Reads a BSON String from the reader.
    /// </summary>
    /// <returns>A String.</returns>
    public abstract String readString();

    /// <summary>
    /// Reads a BSON string element from the reader.
    /// </summary>
    /// <returns>A String.</returns>
    /// <param name="name">The name of the element.</param>
    public String readString(final String name) {
        verifyName(name);
        return readString();
    }

    /// <summary>
    /// Reads a BSON symbol from the reader.
    /// </summary>
    /// <returns>A string.</returns>
    public abstract String readSymbol();

    /// <summary>
    /// Reads a BSON symbol element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    /// <returns>A string.</returns>
    public String readSymbol(final String name) {
        verifyName(name);
        return readSymbol();
    }

    /// <summary>
    /// Reads a BSON timestamp from the reader.
    /// </summary>
    /// <returns>The combined timestamp/increment.</returns>
    public abstract BSONTimestamp readTimestamp();

    /// <summary>
    /// Reads a BSON timestamp element from the reader.
    /// </summary>
    /// <returns>The combined timestamp/increment.</returns>
    /// <param name="name">The name of the element.</param>
    public BSONTimestamp readTimestamp(final String name) {
        verifyName(name);
        return readTimestamp();
    }

    /// <summary>
    /// Reads a BSON undefined from the reader.
    /// </summary>
    public abstract void readUndefined();

    /// <summary>
    /// Reads a BSON undefined element from the reader.
    /// </summary>
    /// <param name="name">The name of the element.</param>
    public void readUndefined(final String name) {
        verifyName(name);
        readUndefined();
    }

    /// <summary>
    /// Returns the reader to previously bookmarked position and state.
    /// </summary>
    /// <param name="bookmark">The bookmark.</param>
//    public abstract void ReturnToBookmark(BsonReaderBookmark bookmark);

    /// <summary>
    /// Skips the name (reader must be positioned on a name).
    /// </summary>
    public abstract void skipName();

    /// <summary>
    /// Skips the value (reader must be positioned on a value).
    /// </summary>
    public abstract void skipValue();

    /// <summary>
    /// Throws an InvalidOperationException when the method called is not valid for the current ContextType.
    /// </summary>
    /// <param name="methodName">The name of the method.</param>
    /// <param name="actualContextType">The actual ContextType.</param>
    /// <param name="validContextTypes">The valid ContextTypes.</param>
    protected void throwInvalidContextType(final String methodName, final ContextType actualContextType,
                                           final ContextType... validContextTypes) {
        final String validContextTypesString = StringUtils.join(" or ", Arrays.asList(validContextTypes));
        final String message = String.format(
                "%s can only be called when ContextType is %s, not when ContextType is %s.",
                methodName, validContextTypesString, actualContextType);
        throw new InvalidOperationException(message);
    }

    /// <summary>
    /// Throws an InvalidOperationException when the method called is not valid for the current state.
    /// </summary>
    /// <param name="methodName">The name of the method.</param>
    /// <param name="validStates">The valid states.</param>
    protected void throwInvalidState(final String methodName, final State... validStates) {
        final String validStatesString = StringUtils.join(" or ", Arrays.asList(validStates));
        final String message = String.format(
                "%s can only be called when State is %s, not when State is %s.",
                methodName, validStatesString, state);
        throw new InvalidOperationException(message);
    }

    /// <summary>
    /// Verifies the current state and BsonType of the reader.
    /// </summary>
    /// <param name="methodName">The name of the method calling this one.</param>
    /// <param name="requiredBsonType">The required BSON type.</param>
    protected void verifyBsonType(final String methodName, final BsonType requiredBsonType) {
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
            final String message = String.format(
                    "%s can only be called when CurrentBsonType is %s, not when CurrentBsonType is %s.",
                    methodName, requiredBsonType, currentBsonType);
            throw new InvalidOperationException(message);
        }
    }

    /// <summary>
    /// Verifies the name of the current element.
    /// </summary>
    /// <param name="expectedName">The expected name.</param>
    protected void verifyName(final String expectedName) {
        readBsonType();
        final String actualName = readName();
        if (!actualName.equals(expectedName)) {
            final String message = String.format(
                    "Expected element name to be '%s', not '%s'.",
                    expectedName, actualName);
            throw new BsonSerializationException(message);
        }
    }

    protected enum State {
        /// <summary>
        /// The initial state.
        /// </summary>
        INITIAL,
        /// <summary>
        /// The reader is positioned at the type of an element or value.
        /// </summary>
        TYPE,
        /// <summary>
        /// The reader is positioned at the name of an element.
        /// </summary>
        NAME,
        /// <summary>
        /// The reader is positioned at a value.
        /// </summary>
        VALUE,
        /// <summary>
        /// The reader is positioned at a scope document.
        /// </summary>
        SCOPE_DOCUMENT,
        /// <summary>
        /// The reader is positioned at the end of a document.
        /// </summary>
        END_OF_DOCUMENT,
        /// <summary>
        /// The reader is positioned at the end of an array.
        /// </summary>
        END_OF_ARRAY,
        /// <summary>
        /// The reader has finished reading a document.
        /// </summary>
        DONE,
        /// <summary>
        /// The reader is closed.
        /// </summary>
        CLOSED
    }
}
