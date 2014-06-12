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
import org.bson.io.OutputBuffer;
import org.bson.types.BsonBinary;
import org.bson.types.BsonDbPointer;
import org.bson.types.BsonRegularExpression;
import org.bson.types.ObjectId;

import java.util.Stack;

/**
 * A BsonWriter implementation that writes to a binary stream of data.  This is the most commonly used implementation.
 *
 * @since 3.0
 */
public class BsonBinaryWriter extends AbstractBsonWriter {
    private final BsonBinaryWriterSettings binaryWriterSettings;

    private final OutputBuffer buffer;
    private final boolean closeBuffer;
    private final Stack<Integer> maxDocumentSizeStack = new Stack<Integer>();
    private Mark mark;

    public BsonBinaryWriter(final OutputBuffer buffer, final FieldNameValidator validator) {
        this(new BsonWriterSettings(), new BsonBinaryWriterSettings(), buffer, validator);
    }

    public BsonBinaryWriter(final OutputBuffer buffer, final boolean closeBuffer) {
        this(new BsonWriterSettings(), new BsonBinaryWriterSettings(), buffer, closeBuffer);
    }

    public BsonBinaryWriter(final BsonWriterSettings settings, final BsonBinaryWriterSettings binaryWriterSettings,
                            final OutputBuffer buffer, final boolean closeBuffer) {
        this(settings, binaryWriterSettings, buffer, new NoOpFieldNameValidator(), closeBuffer);
    }

    public BsonBinaryWriter(final BsonWriterSettings settings, final BsonBinaryWriterSettings binaryWriterSettings,
                            final OutputBuffer buffer, final FieldNameValidator validator) {
        this(settings, binaryWriterSettings, buffer, validator, false);
    }

    private BsonBinaryWriter(final BsonWriterSettings settings, final BsonBinaryWriterSettings binaryWriterSettings,
                             final OutputBuffer buffer, final FieldNameValidator validator, final boolean closeBuffer) {
        super(settings, validator);
        this.binaryWriterSettings = binaryWriterSettings;
        this.buffer = buffer;
        this.closeBuffer = closeBuffer;
        maxDocumentSizeStack.push(binaryWriterSettings.getMaxDocumentSize());
    }

    @Override
    public void close() {
        super.close();
        if (closeBuffer) {
            buffer.close();
        }
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
    public void writeBinaryData(final BsonBinary binary) {
        checkPreconditions("writeBinaryData", State.VALUE);

        buffer.write(BsonType.BINARY.getValue());
        writeCurrentName();

        int totalLen = binary.getData().length;

        if (binary.getType() == BsonBinarySubType.OLD_BINARY.getValue()) {
            totalLen += 4;
        }

        buffer.writeInt(totalLen);
        buffer.write(binary.getType());
        if (binary.getType() == BsonBinarySubType.OLD_BINARY.getValue()) {
            buffer.writeInt(totalLen - 4);
        }
        buffer.write(binary.getData());

        setState(getNextState());
    }

    @Override
    public void writeBoolean(final boolean value) {
        checkPreconditions("writeBoolean", State.VALUE);

        buffer.write(BsonType.BOOLEAN.getValue());
        writeCurrentName();
        buffer.write(value ? 1 : 0);

        setState(getNextState());
    }

    @Override
    public void writeDateTime(final long value) {
        checkPreconditions("writeDateTime", State.VALUE);

        buffer.write(BsonType.DATE_TIME.getValue());
        writeCurrentName();
        buffer.writeLong(value);

        setState(getNextState());
    }

    @Override
    public void writeDouble(final double value) {
        checkPreconditions("writeDouble", State.VALUE);

        buffer.write(BsonType.DOUBLE.getValue());
        writeCurrentName();
        buffer.writeDouble(value);

        setState(getNextState());
    }

    @Override
    public void writeInt32(final int value) {
        checkPreconditions("writeInt32", State.VALUE);

        buffer.write(BsonType.INT32.getValue());
        writeCurrentName();
        buffer.writeInt(value);

        setState(getNextState());
    }

    @Override
    public void writeInt64(final long value) {
        checkPreconditions("writeInt64", State.VALUE);

        buffer.write(BsonType.INT64.getValue());
        writeCurrentName();
        buffer.writeLong(value);

        setState(getNextState());
    }

    @Override
    public void writeJavaScript(final String code) {
        checkPreconditions("writeJavaScript", State.VALUE);

        buffer.write(BsonType.JAVASCRIPT.getValue());
        writeCurrentName();
        buffer.writeString(code);

        setState(getNextState());
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        checkPreconditions("writeJavaScriptWithScope", State.VALUE);

        buffer.write(BsonType.JAVASCRIPT_WITH_SCOPE.getValue());
        writeCurrentName();
        setContext(new Context(getContext(), BsonContextType.JAVASCRIPT_WITH_SCOPE, buffer.getPosition()));
        buffer.writeInt(0);
        buffer.writeString(code);

        setState(State.SCOPE_DOCUMENT);
    }

    @Override
    public void writeMaxKey() {
        checkPreconditions("writeMaxKey", State.VALUE);

        buffer.write(BsonType.MAX_KEY.getValue());
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void writeMinKey() {
        checkPreconditions("writeMinKey", State.VALUE);

        buffer.write(BsonType.MIN_KEY.getValue());
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void writeNull() {
        checkPreconditions("writeNull", State.VALUE);

        buffer.write(BsonType.NULL.getValue());
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        checkPreconditions("writeObjectId", State.VALUE);

        buffer.write(BsonType.OBJECT_ID.getValue());
        writeCurrentName();

        buffer.write(objectId.toByteArray());
        setState(getNextState());
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        checkPreconditions("writeRegularExpression", State.VALUE);

        buffer.write(BsonType.REGULAR_EXPRESSION.getValue());
        writeCurrentName();
        buffer.writeCString(regularExpression.getPattern());
        buffer.writeCString(regularExpression.getOptions());

        setState(getNextState());
    }

    @Override
    public void writeString(final String value) {
        checkPreconditions("writeString", State.VALUE);

        buffer.write(BsonType.STRING.getValue());
        writeCurrentName();
        buffer.writeString(value);

        setState(getNextState());
    }

    @Override
    public void writeSymbol(final String value) {
        checkPreconditions("writeSymbol", State.VALUE);

        buffer.write(BsonType.SYMBOL.getValue());
        writeCurrentName();
        buffer.writeString(value);

        setState(getNextState());
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        checkPreconditions("writeTimestamp", State.VALUE);

        buffer.write(BsonType.TIMESTAMP.getValue());
        writeCurrentName();
        buffer.writeInt(value.getInc());
        buffer.writeInt(value.getTime());

        setState(getNextState());
    }

    @Override
    public void writeUndefined() {
        checkPreconditions("writeUndefined", State.VALUE);

        buffer.write(BsonType.UNDEFINED.getValue());
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public  void writeDBPointer(final BsonDbPointer dbPointer) {
        checkPreconditions("writeDBPointer", State.VALUE);

        buffer.write(BsonType.DB_POINTER.getValue());
        writeCurrentName();

        buffer.writeString(dbPointer.getNamespace());
        buffer.write(dbPointer.getId().toByteArray());

        setState(getNextState());
    }

    @Override
    public void writeStartArray() {
        checkPreconditions("writeStartArray", State.VALUE);

        super.writeStartArray();
        buffer.write(BsonType.ARRAY.getValue());
        writeCurrentName();
        setContext(new Context(getContext(), BsonContextType.ARRAY, buffer.getPosition()));
        buffer.writeInt(0); // reserve space for size

        setState(State.VALUE);
    }

    @Override
    public void writeStartDocument() {
        checkPreconditions("writeStartDocument", State.INITIAL, State.VALUE, State.SCOPE_DOCUMENT, State.DONE);

        super.writeStartDocument();
        if (getState() == State.VALUE) {
            buffer.write(BsonType.DOCUMENT.getValue());
            writeCurrentName();
        }
        setContext(new Context(getContext(), BsonContextType.DOCUMENT, buffer.getPosition()));
        buffer.writeInt(0); // reserve space for size

        setState(State.NAME);
    }

    @Override
    public void writeEndArray() {
        checkPreconditions("writeEndArray", State.VALUE);

        if (getContext().getContextType() != BsonContextType.ARRAY) {
            throwInvalidContextType("WriteEndArray", getContext().getContextType(), BsonContextType.ARRAY);
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

        BsonContextType contextType = getContext().getContextType();

        if (contextType != BsonContextType.DOCUMENT && contextType != BsonContextType.SCOPE_DOCUMENT) {
            throwInvalidContextType("WriteEndDocument", contextType, BsonContextType.DOCUMENT, BsonContextType.SCOPE_DOCUMENT);
        }

        super.writeEndDocument();
        buffer.write(0);
        backpatchSize(); // size of document

        setContext(getContext().getParentContext());
        if (getContext() == null) {
            setState(State.DONE);
        } else {
            if (getContext().getContextType() == BsonContextType.JAVASCRIPT_WITH_SCOPE) {
                backpatchSize(); // size of the JavaScript with scope value
                setContext(getContext().getParentContext());
            }
            setState(getNextState());
        }
    }

    @Override
    public void pipe(final BsonReader reader) {
        if (reader instanceof BsonBinaryReader) {
            BsonBinaryReader binaryReader = (BsonBinaryReader) reader;
            if (getState() == State.VALUE) {
                buffer.write(BsonType.DOCUMENT.getValue());
                writeCurrentName();
            }
            InputBuffer inputBuffer = binaryReader.getBuffer();
            int size = inputBuffer.readInt32();
            buffer.writeInt(size);
            buffer.write(inputBuffer.readBytes(size - 4));
            binaryReader.setState(AbstractBsonReader.State.TYPE);

            if (getContext() == null) {
                setState(State.DONE);
            } else {
                if (getContext().getContextType() == BsonContextType.JAVASCRIPT_WITH_SCOPE) {
                    backpatchSize(); // size of the JavaScript with scope value
                    setContext(getContext().getParentContext());
                }
                setState(getNextState());
            }
        } else {
            super.pipe(reader);
        }
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
        if (getContext().getContextType() == BsonContextType.ARRAY) {
            buffer.writeCString(Integer.toString(getContext().index++));
        } else {
            buffer.writeCString(getName());
        }
    }


    private void backpatchSize() {
        int size = buffer.getPosition() - getContext().startPosition;
        if (size > maxDocumentSizeStack.peek()) {
            String message = String.format("Size %d is larger than MaxDocumentSize %d.", size,
                                           binaryWriterSettings.getMaxDocumentSize());
            throw new BsonSerializationException(message);
        }
        buffer.backpatchSize(size);
    }

    protected class Context extends AbstractBsonWriter.Context {
        private final int startPosition;
        private int index; // used when contextType is an array

        public Context(final Context parentContext, final BsonContextType contextType, final int startPosition) {
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

    protected class Mark extends AbstractBsonWriter.Mark {
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
