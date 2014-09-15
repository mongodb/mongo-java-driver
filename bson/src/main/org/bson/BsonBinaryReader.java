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

import org.bson.io.BsonInputStream;
import org.bson.types.ObjectId;

import static java.lang.String.format;

/**
 * A BsonReader implementation that reads from a binary stream of data.  This is the most commonly used implementation.
 *
 * @since 3.0
 */
public class BsonBinaryReader extends AbstractBsonReader {

    private final BsonInputStream buffer;
    private final boolean closeBuffer;

    public BsonBinaryReader(final BsonInputStream buffer, final boolean closeBuffer) {
        super();
        if (buffer == null) {
            throw new IllegalArgumentException("buffer is null");
        }
        this.buffer = buffer;
        this.closeBuffer = closeBuffer;
        setContext(new Context(null, BsonContextType.TOP_LEVEL, 0, 0));
    }

    @Override
    public void close() {
        super.close();
        if (closeBuffer) {
            buffer.close();
        }
    }

    /**
     * Gets the input buffer backing this instance.
     *
     * @return the input buffer
     */
    public BsonInputStream getBuffer() {
        return buffer;
    }

    @Override
    public BsonType readBsonType() {
        if (isClosed()) {
            throw new IllegalStateException("BSONBinaryWriter");
        }

        if (getState() == State.INITIAL || getState() == State.DONE || getState() == State.SCOPE_DOCUMENT) {
            // there is an implied type of Document for the top level and for scope documents
            setCurrentBsonType(BsonType.DOCUMENT);
            setState(State.VALUE);
            return getCurrentBsonType();
        }
        if (getState() != State.TYPE) {
            throwInvalidState("ReadBSONType", State.TYPE);
        }

        setCurrentBsonType(BsonType.findByValue(buffer.readByte()));

        if (getCurrentBsonType() == BsonType.END_OF_DOCUMENT) {
            switch (getContext().getContextType()) {
                case ARRAY:
                    setState(State.END_OF_ARRAY);
                    return BsonType.END_OF_DOCUMENT;
                case DOCUMENT:
                case SCOPE_DOCUMENT:
                    setState(State.END_OF_DOCUMENT);
                    return BsonType.END_OF_DOCUMENT;
                default:
                    String message = format("BSONType EndOfDocument is not valid when ContextType is %s.",
                                            getContext().getContextType());
                    throw new BsonSerializationException(message);
            }
        } else {
            switch (getContext().getContextType()) {
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
    protected BsonBinary doReadBinaryData() {
        int numBytes = buffer.readInt32();
        byte type = buffer.readByte();

        if (type == BsonBinarySubType.OLD_BINARY.getValue()) {
            buffer.readInt32();
            numBytes -= 4;
        }
        byte[] bytes = new byte[numBytes];
        buffer.readBytes(bytes);
        return new BsonBinary(type, bytes);
    }

    @Override
    protected boolean doReadBoolean() {
        return buffer.readByte() == 0x1;
    }

    @Override
    protected long doReadDateTime() {
        return buffer.readInt64();
    }

    @Override
    protected double doReadDouble() {
        return buffer.readDouble();
    }

    @Override
    protected int doReadInt32() {
        return buffer.readInt32();
    }

    @Override
    protected long doReadInt64() {
        return buffer.readInt64();
    }

    @Override
    protected String doReadJavaScript() {
        return buffer.readString();
    }

    @Override
    protected String doReadJavaScriptWithScope() {
        int startPosition = buffer.getPosition(); // position of size field
        int size = readSize();
        setContext(new Context(getContext(), BsonContextType.JAVASCRIPT_WITH_SCOPE, startPosition, size));
        return buffer.readString();
    }

    @Override
    protected void doReadMaxKey() {
    }

    @Override
    protected void doReadMinKey() {
    }

    @Override
    protected void doReadNull() {
    }

    @Override
    protected ObjectId doReadObjectId() {
        return buffer.readObjectId();
    }

    @Override
    protected BsonRegularExpression doReadRegularExpression() {
        return new BsonRegularExpression(buffer.readCString(), buffer.readCString());
    }

    @Override
    protected BsonDbPointer doReadDBPointer() {
        return new BsonDbPointer(buffer.readString(), buffer.readObjectId());
    }

    @Override
    protected String doReadString() {
        return buffer.readString();
    }

    @Override
    protected String doReadSymbol() {
        return buffer.readString();
    }

    @Override
    protected BsonTimestamp doReadTimestamp() {
        int increment = buffer.readInt32();
        int time = buffer.readInt32();
        return new BsonTimestamp(time, increment);
    }

    @Override
    protected void doReadUndefined() {
    }

    @Override
    public void doReadStartArray() {
        int startPosition = buffer.getPosition(); // position of size field
        int size = readSize();
        setContext(new Context(getContext(), BsonContextType.ARRAY, startPosition, size));
    }

    @Override
    protected void doReadStartDocument() {
        BsonContextType contextType = (getState() == State.SCOPE_DOCUMENT)
                                      ? BsonContextType.SCOPE_DOCUMENT : BsonContextType.DOCUMENT;
        int startPosition = buffer.getPosition(); // position of size field
        int size = readSize();
        setContext(new Context(getContext(), contextType, startPosition, size));
    }

    @Override
    protected void doReadEndArray() {
        setContext(getContext().popContext(buffer.getPosition()));
    }

    @Override
    protected void doReadEndDocument() {
        setContext(getContext().popContext(buffer.getPosition()));
        if (getContext().getContextType() == BsonContextType.JAVASCRIPT_WITH_SCOPE) {
            setContext(getContext().popContext(buffer.getPosition())); // JavaScriptWithScope
        }
    }

    @Override
    protected void doSkipName() {
    }

    @Override
    protected void doSkipValue() {
        if (isClosed()) {
            throw new IllegalStateException("BSONBinaryWriter");
        }
        if (getState() != State.VALUE) {
            throwInvalidState("skipValue", State.VALUE);
        }

        int skip;
        switch (getCurrentBsonType()) {
            case ARRAY:
                skip = readSize() - 4;
                break;
            case BINARY:
                skip = readSize() + 1;
                break;
            case BOOLEAN:
                skip = 1;
                break;
            case DATE_TIME:
                skip = 8;
                break;
            case DOCUMENT:
                skip = readSize() - 4;
                break;
            case DOUBLE:
                skip = 8;
                break;
            case INT32:
                skip = 4;
                break;
            case INT64:
                skip = 8;
                break;
            case JAVASCRIPT:
                skip = readSize();
                break;
            case JAVASCRIPT_WITH_SCOPE:
                skip = readSize() - 4;
                break;
            case MAX_KEY:
                skip = 0;
                break;
            case MIN_KEY:
                skip = 0;
                break;
            case NULL:
                skip = 0;
                break;
            case OBJECT_ID:
                skip = 12;
                break;
            case REGULAR_EXPRESSION:
                buffer.skipCString();
                buffer.skipCString();
                skip = 0;
                break;
            case STRING:
                skip = readSize();
                break;
            case SYMBOL:
                skip = readSize();
                break;
            case TIMESTAMP:
                skip = 8;
                break;
            case UNDEFINED:
                skip = 0;
                break;
            default:
                throw new BSONException("Unexpected BSON type: " + getCurrentBsonType());
        }
        buffer.skip(skip);

        setState(State.TYPE);
    }

    private int readSize() {
        int size = buffer.readInt32();
        if (size < 0) {
            String message = format("Size %s is not valid because it is negative.", size);
            throw new BsonSerializationException(message);
        }
        return size;
    }

    protected Context getContext() {
        return (Context) super.getContext();
    }


    private static class Context extends AbstractBsonReader.Context {
        private final int startPosition;
        private final int size;

        Context(final Context parentContext, final BsonContextType contextType, final int startPosition, final int size) {
            super(parentContext, contextType);
            this.startPosition = startPosition;
            this.size = size;
        }

        Context popContext(final int position) {
            int actualSize = position - startPosition;
            if (actualSize != size) {
                String message = format("Expected size to be %d, not %d.", size, actualSize);
                throw new BsonSerializationException(message);
            }
            return getParentContext();
        }

        @Override
        protected Context getParentContext() {
            return (Context) super.getParentContext();
        }
    }
}
