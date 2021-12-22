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

import org.bson.io.BsonInput;
import org.bson.io.BsonInputMark;
import org.bson.io.ByteBufferBsonInput;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;

/**
 * A BsonReader implementation that reads from a binary stream of data.  This is the most commonly used implementation.
 *
 * @since 3.0
 */
public class BsonBinaryReader extends AbstractBsonReader {

    private final BsonInput bsonInput;

    /**
     * Construct an instance.
     *
     * @param byteBuffer the input for this reader
     */
    public BsonBinaryReader(final ByteBuffer byteBuffer) {
        this(new ByteBufferBsonInput(new ByteBufNIO(notNull("byteBuffer", byteBuffer))));
    }

    /**
     * Construct an instance.
     *
     * @param bsonInput the input for this reader
     */
    public BsonBinaryReader(final BsonInput bsonInput) {
        if (bsonInput == null) {
            throw new IllegalArgumentException("bsonInput is null");
        }
        this.bsonInput = bsonInput;
        setContext(new Context(null, BsonContextType.TOP_LEVEL, 0, 0));
    }

    @Override
    public void close() {
        super.close();
    }

    /**
     * Gets the BSON input backing this instance.
     *
     * @return the BSON input
     */
    public BsonInput getBsonInput() {
        return bsonInput;
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

        byte bsonTypeByte = bsonInput.readByte();
        BsonType bsonType = BsonType.findByValue(bsonTypeByte);
        if (bsonType == null) {
            String name = bsonInput.readCString();
            throw new BsonSerializationException(format("Detected unknown BSON type \"\\x%x\" for fieldname \"%s\". "
                    + "Are you using the latest driver version?",
                    bsonTypeByte, name));
        }
        setCurrentBsonType(bsonType);

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
                    throw new BsonSerializationException(format("BSONType EndOfDocument is not valid when ContextType is %s.",
                            getContext().getContextType()));
            }
        } else {
            switch (getContext().getContextType()) {
                case ARRAY:
                    bsonInput.skipCString(); // ignore array element names
                    setState(State.VALUE);
                    break;
                case DOCUMENT:
                case SCOPE_DOCUMENT:
                    setCurrentName(bsonInput.readCString());
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
        int numBytes = readSize();
        byte type = bsonInput.readByte();

        if (type == BsonBinarySubType.OLD_BINARY.getValue()) {
            int repeatedNumBytes = bsonInput.readInt32();
            if (repeatedNumBytes != numBytes - 4) {
                throw new BsonSerializationException("Binary sub type OldBinary has inconsistent sizes");
            }
            numBytes -= 4;
        }
        byte[] bytes = new byte[numBytes];
        bsonInput.readBytes(bytes);
        return new BsonBinary(type, bytes);
    }

    @Override
    protected byte doPeekBinarySubType() {
        Mark mark = new Mark();
        readSize();
        byte type = bsonInput.readByte();
        mark.reset();
        return type;
    }

    @Override
    protected int doPeekBinarySize() {
        Mark mark = new Mark();
        int size = readSize();
        mark.reset();
        return size;
    }

    @Override
    protected boolean doReadBoolean() {
        byte booleanByte = bsonInput.readByte();
        if (booleanByte != 0 && booleanByte != 1) {
            throw new BsonSerializationException(format("Expected a boolean value but found %d", booleanByte));
        }
        return booleanByte == 0x1;
    }

    @Override
    protected long doReadDateTime() {
        return bsonInput.readInt64();
    }

    @Override
    protected double doReadDouble() {
        return bsonInput.readDouble();
    }

    @Override
    protected int doReadInt32() {
        return bsonInput.readInt32();
    }

    @Override
    protected long doReadInt64() {
        return bsonInput.readInt64();
    }

    @Override
    public Decimal128 doReadDecimal128() {
        long low = bsonInput.readInt64();
        long high = bsonInput.readInt64();
        return Decimal128.fromIEEE754BIDEncoding(high, low);
    }

    @Override
    protected String doReadJavaScript() {
        return bsonInput.readString();
    }

    @Override
    protected String doReadJavaScriptWithScope() {
        int startPosition = bsonInput.getPosition(); // position of size field
        int size = readSize();
        setContext(new Context(getContext(), BsonContextType.JAVASCRIPT_WITH_SCOPE, startPosition, size));
        return bsonInput.readString();
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
        return bsonInput.readObjectId();
    }

    @Override
    protected BsonRegularExpression doReadRegularExpression() {
        return new BsonRegularExpression(bsonInput.readCString(), bsonInput.readCString());
    }

    @Override
    protected BsonDbPointer doReadDBPointer() {
        return new BsonDbPointer(bsonInput.readString(), bsonInput.readObjectId());
    }

    @Override
    protected String doReadString() {
        return bsonInput.readString();
    }

    @Override
    protected String doReadSymbol() {
        return bsonInput.readString();
    }

    @Override
    protected BsonTimestamp doReadTimestamp() {
        return new BsonTimestamp(bsonInput.readInt64());
    }

    @Override
    protected void doReadUndefined() {
    }

    @Override
    public void doReadStartArray() {
        int startPosition = bsonInput.getPosition(); // position of size field
        int size = readSize();
        setContext(new Context(getContext(), BsonContextType.ARRAY, startPosition, size));
    }

    @Override
    protected void doReadStartDocument() {
        BsonContextType contextType = (getState() == State.SCOPE_DOCUMENT)
                ? BsonContextType.SCOPE_DOCUMENT : BsonContextType.DOCUMENT;
        int startPosition = bsonInput.getPosition(); // position of size field
        int size = readSize();
        setContext(new Context(getContext(), contextType, startPosition, size));
    }

    @Override
    protected void doReadEndArray() {
        setContext(getContext().popContext(bsonInput.getPosition()));
    }

    @Override
    protected void doReadEndDocument() {
        setContext(getContext().popContext(bsonInput.getPosition()));
        if (getContext().getContextType() == BsonContextType.JAVASCRIPT_WITH_SCOPE) {
            setContext(getContext().popContext(bsonInput.getPosition())); // JavaScriptWithScope
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
            case DECIMAL128:
                skip = 16;
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
                bsonInput.skipCString();
                bsonInput.skipCString();
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
            case DB_POINTER:
                skip = readSize() + 12;   // String followed by ObjectId
                break;
            default:
                throw new BSONException("Unexpected BSON type: " + getCurrentBsonType());
        }
        bsonInput.skip(skip);

        setState(State.TYPE);
    }

    private int readSize() {
        int size = bsonInput.readInt32();
        if (size < 0) {
            String message = format("Size %s is not valid because it is negative.", size);
            throw new BsonSerializationException(message);
        }
        return size;
    }

    protected Context getContext() {
        return (Context) super.getContext();
    }

    @Override
    public BsonReaderMark getMark() {
        return new Mark();
    }

    /**
     * An implementation of {@code AbstractBsonReader.Mark}.
     */
    protected class Mark extends AbstractBsonReader.Mark {
        private final int startPosition;
        private final int size;
        private final BsonInputMark bsonInputMark;

        /**
         * Construct an instance.
         */
        protected Mark() {
            super();
            startPosition = BsonBinaryReader.this.getContext().startPosition;
            size = BsonBinaryReader.this.getContext().size;
            bsonInputMark = BsonBinaryReader.this.bsonInput.getMark(Integer.MAX_VALUE);
        }

        @Override
        public void reset() {
            super.reset();
            bsonInputMark.reset();
            BsonBinaryReader.this.setContext(new Context((Context) getParentContext(), getContextType(), startPosition, size));
        }
    }

    /**
     * An implementation of {@code AbstractBsonReader.Context}.
     */
    protected class Context extends AbstractBsonReader.Context {
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
                throw new BsonSerializationException(format("Expected size to be %d, not %d.", size, actualSize));
            }
            return getParentContext();
        }

        @Override
        protected Context getParentContext() {
            return (Context) super.getParentContext();
        }
    }
}
