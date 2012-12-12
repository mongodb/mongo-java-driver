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

import org.bson.io.InputBuffer;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;

import static org.bson.BSON.B_BINARY;

public class BSONBinaryReader extends BSONReader {

    private Context context;
    private final InputBuffer buffer;

    public BSONBinaryReader(BsonReaderSettings settings, InputBuffer buffer) {
        super(settings);
        this.buffer = buffer;
        context = new Context(null, ContextType.TOP_LEVEL, 0, 0);
    }

    @Override
    public BsonType readBsonType() {
        if (isClosed()) {
            throw new IllegalStateException("BsonBinaryWriter");
        }

        if (getState() == State.INITIAL || getState() == State.DONE || getState() == State.SCOPE_DOCUMENT) {
            // there is an implied type of Document for the top level and for scope documents
            setCurrentBsonType(BsonType.DOCUMENT);
            setState(State.VALUE);
            return getCurrentBsonType();
        }
        if (getState() != State.TYPE) {
            throwInvalidState("ReadBsonType", State.TYPE);
        }

        setCurrentBsonType(buffer.readBsonType());

        if (getCurrentBsonType() == BsonType.END_OF_DOCUMENT) {
            switch (context.contextType) {
                case ARRAY:
                    setState(State.END_OF_ARRAY);
                    return BsonType.END_OF_DOCUMENT;
                case DOCUMENT:
                case SCOPE_DOCUMENT:
                    setState(State.END_OF_DOCUMENT);
                    return BsonType.END_OF_DOCUMENT;
                default:
                    String message = String.format("BsonType EndOfDocument is not valid when ContextType is %s.", context.contextType);
                    throw new BsonSerializationException(message);
            }
        }
        else {
            switch (context.contextType) {
                case ARRAY:
                    buffer.skipCString(); // ignore array element names
                    setState(State.VALUE);
                    break;
                case DOCUMENT:
                case SCOPE_DOCUMENT:
                    setCurrentName(buffer.readCString());
                    setState(State.NAME);
                    break;
                default:
                    throw new BSONException("Unexpected ContextType.");
            }

            return getCurrentBsonType();
        }
    }

    @Override
    public Binary readBinaryData() {
        checkPreconditions("readBinaryDate", BsonType.BINARY);
        setState(getNextState());

        // TODO: implement
        int numBytes = buffer.readInt32();
        byte type = buffer.readByte();

        if (type == B_BINARY) {
            buffer.readInt32();
            numBytes -= 4;
        }

        byte[] data = buffer.readBytes(numBytes);
        return new Binary(type, data);
    }

    @Override
    public boolean readBoolean() {
        checkPreconditions("readBoolean", BsonType.BOOLEAN);
        setState(getNextState());
        return buffer.readBoolean();
    }

    @Override
    public long readDateTime() {
        checkPreconditions("readDateTime", BsonType.DATE_TIME);
        setState(getNextState());
        return buffer.readInt64();
    }

    @Override
    public double readDouble() {
        checkPreconditions("readDouble", BsonType.DOUBLE);
        setState(getNextState());
        return buffer.readDouble();
    }

    @Override
    public int readInt32() {
        checkPreconditions("readInt32", BsonType.INT32);
        setState(getNextState());
        return buffer.readInt32();
    }

    @Override
    public long readInt64() {
        checkPreconditions("readInt64", BsonType.INT64);
        setState(getNextState());
        return buffer.readInt64();
    }

    @Override
    public String readJavaScript() {
        checkPreconditions("readJavaScript", BsonType.JAVASCRIPT);
        setState(getNextState());
        return buffer.readString();
    }

    @Override
    public String readJavaScriptWithScope() {
        checkPreconditions("readInt64", BsonType.JAVASCRIPT_WITH_SCOPE);
        setState(getNextState());

        int startPosition = buffer.getPosition(); // position of size field
        int size = readSize();
        context = new Context(context, ContextType.JAVASCRIPT_WITH_SCOPE, startPosition, size);
        String code = buffer.readString();

        setState(State.SCOPE_DOCUMENT);
        return code;
    }

    @Override
    public void readMaxKey() {
        checkPreconditions("readMaxKey", BsonType.MAX_KEY);
        setState(getNextState());
    }

    @Override
    public void readMinKey() {
        checkPreconditions("readMinKey", BsonType.MIN_KEY);
        setState(getNextState());
    }

    @Override
    public void readNull() {
        checkPreconditions("readNull", BsonType.NULL);
        setState(getNextState());
    }

    @Override
    public ObjectId readObjectId() {
        checkPreconditions("readObjectId", BsonType.OBJECT_ID);
        setState(getNextState());
        return buffer.readObjectId();
    }

    @Override
    public RegularExpression readRegularExpression() {
        checkPreconditions("readRegularExpression", BsonType.REGULAR_EXPRESSION);
        setState(getNextState());
        return new RegularExpression(buffer.readCString(), buffer.readCString());
    }

    @Override
    public String readString() {
        checkPreconditions("readString", BsonType.STRING);
        setState(getNextState());
        return buffer.readString();
    }

    @Override
    public String readSymbol() {
        checkPreconditions("readSymbol", BsonType.SYMBOL);
        setState(getNextState());
        return buffer.readString();
    }

    @Override
    public long readTimestamp() {
        checkPreconditions("readTimestamp", BsonType.TIMESTAMP);
        setState(getNextState());
        return buffer.readInt64();
    }

    @Override
    public void readUndefined() {
        checkPreconditions("readUndefined", BsonType.UNDEFINED);
        setState(getNextState());
    }

    @Override
    public void readStartArray() {
        checkPreconditions("readStartArray", BsonType.ARRAY);

        int startPosition = buffer.getPosition(); // position of size field
        int size = readSize();
        context = new Context(context, ContextType.ARRAY, startPosition, size);
        setState(State.TYPE);
    }

    @Override
    public void readStartDocument() {
        checkPreconditions("readStartDocument", BsonType.DOCUMENT);

        ContextType contextType = (getState() == State.SCOPE_DOCUMENT) ? ContextType.SCOPE_DOCUMENT : ContextType.DOCUMENT;
        int startPosition = buffer.getPosition(); // position of size field
        int size = readSize();
        context = new Context(context, contextType, startPosition, size);
        setState(State.TYPE);
    }

    @Override
    public void readEndArray() {
        if (isClosed()) {
            throw new IllegalStateException("BsonBinaryWriter");
        }
        if (context.contextType != ContextType.ARRAY) {
            throwInvalidContextType("readEndArray", context.contextType, ContextType.ARRAY);
        }
        if (getState() == State.TYPE) {
            readBsonType(); // will set state to EndOfArray if at end of array
        }
        if (getState() != State.END_OF_ARRAY) {
            throwInvalidState("ReadEndArray", State.END_OF_ARRAY);
        }

        context = context.popContext(buffer.getPosition());
        setStateOnEnd();
    }

    @Override
    public void readEndDocument() {
        if (isClosed()) {
            throw new IllegalStateException("BsonBinaryWriter");
        }
        if (context.contextType != ContextType.DOCUMENT && context.contextType != ContextType.SCOPE_DOCUMENT) {
            throwInvalidContextType("readEndDocument", context.contextType, ContextType.DOCUMENT, ContextType.SCOPE_DOCUMENT);
        }
        if (getState() == State.TYPE) {
            readBsonType(); // will set state to EndOfDocument if at end of document
        }
        if (getState() != State.END_OF_DOCUMENT) {
            throwInvalidState("readEndDocument", State.END_OF_DOCUMENT);
        }

        context = context.popContext(buffer.getPosition());
        if (context != null && context.contextType == ContextType.JAVASCRIPT_WITH_SCOPE) {
            context = context.popContext(buffer.getPosition()); // JavaScriptWithScope
        }

        setStateOnEnd();
    }

    private void setStateOnEnd() {
        switch (context.contextType) {
            case ARRAY:
            case DOCUMENT:
                setState(State.TYPE);
                break;
            case TOP_LEVEL:
                setState(State.DONE);
                break;
            default:
                throw new BSONException(String.format("Unexpected ContextType %s.", context.contextType));
        }
    }

    @Override
    public void skipName() {
        // TODO: implement
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipValue() {
        // TODO: implement
        throw new UnsupportedOperationException();
    }

    private void checkPreconditions(String methodName, BsonType type) {
        if (isClosed()) {
            throw new IllegalStateException("BsonBinaryWriter");
        }

        verifyBsonType(methodName, type);
    }

    private State getNextState() {
        switch (context.contextType) {
            case ARRAY:
            case DOCUMENT:
            case SCOPE_DOCUMENT:
                return State.TYPE;
            case TOP_LEVEL:
                return State.DONE;
            default:
                throw new BSONException(String.format("Unexpected ContextType %s.", context.contextType));
        }
    }

    private int readSize() {
        int size = buffer.readInt32();
        if (size < 0) {
            String message = String.format("Size %s is not valid because it is negative.", size);
            throw new BsonSerializationException(message);
        }
        return size;
    }


    private static class Context {
        // private fields
        private final Context parentContext;
        private final ContextType contextType;
        private final int startPosition;
        private final int size;

        // constructors
        Context(Context parentContext, ContextType contextType, int startPosition, int size) {
            this.parentContext = parentContext;
            this.contextType = contextType;
            this.startPosition = startPosition;
            this.size = size;
        }

        // public methods
        /// <summary>
        /// Creates a clone of the context.
        /// </summary>
        /// <returns>A clone of the context.</returns>
        public Context copy() {
            return new Context(parentContext, contextType, startPosition, size);
        }

        Context popContext(int position) {
            int actualSize = position - startPosition;
            if (actualSize != size) {
                String message = String.format("Expected size to be %d, not %d.", size, actualSize);
                throw new BsonSerializationException(message);
            }
            return parentContext;
        }
    }
}
