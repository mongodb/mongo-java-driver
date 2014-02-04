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

package com.mongodb;

import org.bson.io.OutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.util.Stack;

import static org.bson.util.Assertions.isTrue;

class BSONBinaryWriter extends BSONWriter {
    private final BSONBinaryWriterSettings binaryWriterSettings;

    private final OutputBuffer buffer;
    private final Stack<Integer> maxDocumentSizeStack = new Stack<Integer>();
    private Mark mark;

    public BSONBinaryWriter(final OutputBuffer buffer) {
        this(new BSONWriterSettings(), new BSONBinaryWriterSettings(), buffer);
    }

    public BSONBinaryWriter(final BSONWriterSettings settings, final BSONBinaryWriterSettings binaryWriterSettings,
                            final OutputBuffer buffer) {
        super(settings);
        this.binaryWriterSettings = binaryWriterSettings;
        this.buffer = buffer;
        maxDocumentSizeStack.push(binaryWriterSettings.getMaxDocumentSize());
    }

    @Override
    public void close() {
        super.close();
    }

    /**
     * Gets the output buffer that is backing this instance.
     *
     * @return the buffer
     */
    public OutputBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void flush() {
    }

    @Override
    protected Context getContext() {
        return (Context) super.getContext();
    }

    @Override
    public void writeBinaryData(final Binary binary) {
        checkPreconditions("writeBinaryData", State.VALUE);

        buffer.write(BSONType.BINARY.getValue());
        writeCurrentName();

        int totalLen = binary.length();

        if (binary.getType() == BSONBinarySubType.OldBinary.getValue()) {
            totalLen += 4;
        }

        buffer.writeInt(totalLen);
        buffer.write(binary.getType());
        if (binary.getType() == BSONBinarySubType.OldBinary.getValue()) {
            buffer.writeInt(totalLen - 4);
        }
        buffer.write(binary.getData());

        setState(getNextState());
    }

    @Override
    public void writeBoolean(final boolean value) {
        checkPreconditions("writeBoolean", State.VALUE);

        buffer.write(BSONType.BOOLEAN.getValue());
        writeCurrentName();
        buffer.write(value ? 1 : 0);

        setState(getNextState());
    }

    @Override
    public void writeDateTime(final long value) {
        checkPreconditions("writeDateTime", State.VALUE);

        buffer.write(BSONType.DATE_TIME.getValue());
        writeCurrentName();
        buffer.writeLong(value);

        setState(getNextState());
    }

    @Override
    public void writeDouble(final double value) {
        checkPreconditions("writeDouble", State.VALUE);

        buffer.write(BSONType.DOUBLE.getValue());
        writeCurrentName();
        buffer.writeDouble(value);

        setState(getNextState());
    }

    @Override
    public void writeInt32(final int value) {
        checkPreconditions("writeInt32", State.VALUE);

        buffer.write(BSONType.INT32.getValue());
        writeCurrentName();
        buffer.writeInt(value);

        setState(getNextState());
    }

    @Override
    public void writeInt64(final long value) {
        checkPreconditions("writeInt64", State.VALUE);

        buffer.write(BSONType.INT64.getValue());
        writeCurrentName();
        buffer.writeLong(value);

        setState(getNextState());
    }

    @Override
    public void writeJavaScript(final String code) {
        checkPreconditions("writeJavaScript", State.VALUE);

        buffer.write(BSONType.JAVASCRIPT.getValue());
        writeCurrentName();
        buffer.writeString(code);

        setState(getNextState());
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        checkPreconditions("writeJavaScriptWithScope", State.VALUE);

        buffer.write(BSONType.JAVASCRIPT_WITH_SCOPE.getValue());
        writeCurrentName();
        setContext(new Context(getContext(), BSONContextType.JAVASCRIPT_WITH_SCOPE, buffer.getPosition()));
        buffer.writeInt(0);
        buffer.writeString(code);

        setState(State.SCOPE_DOCUMENT);
    }

    @Override
    public void writeMaxKey() {
        checkPreconditions("writeMaxKey", State.VALUE);

        buffer.write(BSONType.MAX_KEY.getValue());
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void writeMinKey() {
        checkPreconditions("writeMinKey", State.VALUE);

        buffer.write(BSONType.MIN_KEY.getValue());
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void writeNull() {
        checkPreconditions("writeNull", State.VALUE);

        buffer.write(BSONType.NULL.getValue());
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        checkPreconditions("writeObjectId", State.VALUE);

        buffer.write(BSONType.OBJECT_ID.getValue());
        writeCurrentName();

        buffer.write(objectId.toByteArray());
        setState(getNextState());
    }

    @Override
    public void writeString(final String value) {
        checkPreconditions("writeString", State.VALUE);

        buffer.write(BSONType.STRING.getValue());
        writeCurrentName();
        buffer.writeString(value);

        setState(getNextState());
    }

    @Override
    public void writeSymbol(final String value) {
        checkPreconditions("writeSymbol", State.VALUE);

        buffer.write(BSONType.SYMBOL.getValue());
        writeCurrentName();
        buffer.writeString(value);

        setState(getNextState());
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void writeTimestamp(final BSONTimestamp value) {
        checkPreconditions("writeTimestamp", State.VALUE);

        buffer.write(BSONType.TIMESTAMP.getValue());
        writeCurrentName();
        buffer.writeInt(value.getInc());
        buffer.writeInt(value.getTime());

        setState(getNextState());
    }

    @Override
    public void writeUndefined() {
        checkPreconditions("writeUndefined", State.VALUE);

        buffer.write(BSONType.UNDEFINED.getValue());
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void writeStartArray() {
        checkPreconditions("writeStartArray", State.VALUE);

        super.writeStartArray();
        buffer.write(BSONType.ARRAY.getValue());
        writeCurrentName();
        setContext(new Context(getContext(), BSONContextType.ARRAY, buffer.getPosition()));
        buffer.writeInt(0); // reserve space for size

        setState(State.VALUE);
    }

    @Override
    public void writeStartDocument() {
        checkPreconditions("writeStartDocument", State.INITIAL, State.VALUE, State.SCOPE_DOCUMENT, State.DONE);

        super.writeStartDocument();
        if (getState() == State.VALUE) {
            buffer.write(BSONType.DOCUMENT.getValue());
            writeCurrentName();
        }
        setContext(new Context(getContext(), BSONContextType.DOCUMENT, buffer.getPosition()));
        buffer.writeInt(0); // reserve space for size

        setState(State.NAME);
    }

    @Override
    public void writeEndArray() {
        checkPreconditions("writeEndArray", State.VALUE);

        if (getContext().getContextType() != BSONContextType.ARRAY) {
            throwInvalidContextType("WriteEndArray", getContext().getContextType(), BSONContextType.ARRAY);
        }

        super.writeEndArray();
        buffer.write(0);
        backpatchSize(); // size of document

        setContext(getContext().getParentContext());
        setState(getNextState());
    }

    @Override
    public void writeEndDocument() {
        checkPreconditions("writeEndDocument", State.NAME);

        BSONContextType contextType = getContext().getContextType();

        if (contextType != BSONContextType.DOCUMENT && contextType != BSONContextType.SCOPE_DOCUMENT) {
            throwInvalidContextType("WriteEndDocument", contextType, BSONContextType.DOCUMENT, BSONContextType.SCOPE_DOCUMENT);
        }

        super.writeEndDocument();
        buffer.write(0);
        backpatchSize(); // size of document

        setContext(getContext().getParentContext());
        if (getContext() == null) {
            setState(State.DONE);
        }
        else {
            if (getContext().getContextType() == BSONContextType.JAVASCRIPT_WITH_SCOPE) {
                backpatchSize(); // size of the JavaScript with scope value
                setContext(getContext().getParentContext());
            }
            setState(getNextState());
        }
    }

    public void encodeDocument(final DBEncoder encoder, final DBObject dbObject) {
        checkPreconditions("writeStartDocument", State.INITIAL, State.VALUE, State.SCOPE_DOCUMENT, State.DONE);
        isTrue("state is VALUE", getState() == State.VALUE);
        isTrue("context not null", getContext() != null);
        isTrue("context is not JAVASCRIPT_WITH_SCOPE", getContext().getContextType() != BSONContextType.JAVASCRIPT_WITH_SCOPE);

        buffer.write(BSONType.DOCUMENT.getValue());
        writeCurrentName();

        int startPos = buffer.getPosition();
        encoder.writeObject(buffer, dbObject);
        throwIfSizeExceedsLimit(buffer.getPosition() - startPos);

        setState(getNextState());
    }

    public void pushMaxDocumentSize(final int maxDocumentSize) {
        maxDocumentSizeStack.push(maxDocumentSize);
    }

    public void popMaxDocumentSize() {
        maxDocumentSizeStack.pop();
    }

    public void mark() {
        mark = new Mark();
    }

    public void reset() {
        if (mark == null) {
            throw new IllegalStateException("Can not reset without first marking");
        }

        mark.reset();
        mark = null;
    }

    private void writeCurrentName() {
        if (getContext().getContextType() == BSONContextType.ARRAY) {
            buffer.writeCString(Integer.toString(getContext().index++));
        }
        else {
            buffer.writeCString(getName());
        }
    }

    private void backpatchSize() {
        final int size = buffer.getPosition() - getContext().startPosition;
        throwIfSizeExceedsLimit(size);
        buffer.backpatchSize(size);
    }

    private void throwIfSizeExceedsLimit(final int size) {
        if (size > maxDocumentSizeStack.peek()) {
            final String message = String.format("Size %d is larger than MaxDocumentSize %d.",
                                                 buffer.getPosition() - getContext().startPosition,
                                                 binaryWriterSettings.getMaxDocumentSize());
            throw new MongoInternalException(message);
        }
    }

    class Context extends BSONWriter.Context {
        private final int startPosition;
        private int index; // used when contextType is an array

        public Context(final Context parentContext, final BSONContextType contextType, final int startPosition) {
            super(parentContext, contextType);
            this.startPosition = startPosition;
        }

        public Context(final Context from) {
            super(from);
            startPosition = from.startPosition;
            index = from.index;
        }

        @Override
        public Context getParentContext() {
            return (Context) super.getParentContext();
        }

        @Override
        public Context copy() {
            return new Context(this);
        }
    }

    class Mark extends BSONWriter.Mark {
        private final int position;

        protected Mark() {
            this.position = buffer.getPosition();
        }

        protected void reset() {
            super.reset();
            buffer.truncateToPosition(mark.position);
        }
    }
}
