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

import org.bson.io.InputBuffer;
import org.bson.types.Binary;
import org.bson.types.DBPointer;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.bson.types.Timestamp;

import static java.lang.String.format;

public class BSONBinaryReader extends AbstractBSONReader {

    private final InputBuffer buffer;
    private final boolean closeBuffer;

    public BSONBinaryReader(final BSONReaderSettings settings, final InputBuffer buffer, final boolean closeBuffer) {
        super(settings);
        if (buffer == null) {
            throw new IllegalArgumentException("buffer is null");
        }
        this.buffer = buffer;
        this.closeBuffer = closeBuffer;
        setContext(new Context(null, BSONContextType.TOP_LEVEL, 0, 0));
    }

    public BSONBinaryReader(final InputBuffer buffer, final boolean closeBuffer) {
        this(new BSONReaderSettings(), buffer, closeBuffer);
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
    public InputBuffer getBuffer() {
        return buffer;
    }

    @Override
    public BSONType readBSONType() {
        if (isClosed()) {
            throw new IllegalStateException("BSONBinaryWriter");
        }

        if (getState() == State.INITIAL || getState() == State.DONE || getState() == State.SCOPE_DOCUMENT) {
            // there is an implied type of Document for the top level and for scope documents
            setCurrentBSONType(BSONType.DOCUMENT);
            setState(State.VALUE);
            return getCurrentBSONType();
        }
        if (getState() != State.TYPE) {
            throwInvalidState("ReadBSONType", State.TYPE);
        }

        setCurrentBSONType(buffer.readBSONType());

        if (getCurrentBSONType() == BSONType.END_OF_DOCUMENT) {
            switch (getContext().getContextType()) {
                case ARRAY:
                    setState(State.END_OF_ARRAY);
                    return BSONType.END_OF_DOCUMENT;
                case DOCUMENT:
                case SCOPE_DOCUMENT:
                    setState(State.END_OF_DOCUMENT);
                    return BSONType.END_OF_DOCUMENT;
                default:
                    String message = format("BSONType EndOfDocument is not valid when ContextType is %s.",
                                            getContext().getContextType());
                    throw new BSONSerializationException(message);
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

            return getCurrentBSONType();
        }
    }

    @Override
    protected Binary doReadBinaryData() {
        int numBytes = buffer.readInt32();
        byte type = buffer.readByte();

        if (type == BSONBinarySubType.OLD_BINARY.getValue()) {
            buffer.readInt32();
            numBytes -= 4;
        }

        return new Binary(type, buffer.readBytes(numBytes));
    }

    @Override
    protected boolean doReadBoolean() {
        return buffer.readBoolean();
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
        setContext(new Context(getContext(), BSONContextType.JAVASCRIPT_WITH_SCOPE, startPosition, size));
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
    protected RegularExpression doReadRegularExpression() {
        return new RegularExpression(buffer.readCString(), buffer.readCString());
    }

    @Override
    protected DBPointer doReadDBPointer() {
        return new DBPointer(buffer.readString(), buffer.readObjectId());
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
    protected Timestamp doReadTimestamp() {
        int increment = buffer.readInt32();
        int time = buffer.readInt32();
        return new Timestamp(time, increment);
    }

    @Override
    protected void doReadUndefined() {
    }

    @Override
    public void doReadStartArray() {
        int startPosition = buffer.getPosition(); // position of size field
        int size = readSize();
        setContext(new Context(getContext(), BSONContextType.ARRAY, startPosition, size));
    }

    @Override
    protected void doReadStartDocument() {
        BSONContextType contextType = (getState() == State.SCOPE_DOCUMENT)
                                      ? BSONContextType.SCOPE_DOCUMENT : BSONContextType.DOCUMENT;
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
        if (getContext().getContextType() == BSONContextType.JAVASCRIPT_WITH_SCOPE) {
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
        switch (getCurrentBSONType()) {
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
                throw new BSONException("Unexpected BSON type: " + getCurrentBSONType());
        }
        buffer.skip(skip);

        setState(State.TYPE);
    }

    private int readSize() {
        int size = buffer.readInt32();
        if (size < 0) {
            String message = format("Size %s is not valid because it is negative.", size);
            throw new BSONSerializationException(message);
        }
        return size;
    }

    protected Context getContext() {
        return (Context) super.getContext();
    }


    private static class Context extends AbstractBSONReader.Context {
        private final int startPosition;
        private final int size;

        Context(final Context parentContext, final BSONContextType contextType, final int startPosition, final int size) {
            super(parentContext, contextType);
            this.startPosition = startPosition;
            this.size = size;
        }

        Context popContext(final int position) {
            int actualSize = position - startPosition;
            if (actualSize != size) {
                String message = format("Expected size to be %d, not %d.", size, actualSize);
                throw new BSONSerializationException(message);
            }
            return getParentContext();
        }

        @Override
        protected Context getParentContext() {
            return (Context) super.getParentContext();
        }
    }
}
