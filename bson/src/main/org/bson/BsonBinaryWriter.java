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
import org.bson.io.OutputBuffer;
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
    protected void doWriteStartDocument() {
        if (getState() == State.VALUE) {
            buffer.write(BsonType.DOCUMENT.getValue());
            writeCurrentName();
        }
        setContext(new Context(getContext(), BsonContextType.DOCUMENT, buffer.getPosition()));
        buffer.writeInt(0); // reserve space for size
    }

    @Override
    protected void doWriteEndDocument() {
        buffer.write(0);
        backpatchSize(); // size of document

        setContext(getContext().getParentContext());
        if (getContext() != null && getContext().getContextType() == BsonContextType.JAVASCRIPT_WITH_SCOPE) {
            backpatchSize(); // size of the JavaScript with scope value
            setContext(getContext().getParentContext());
        }
    }

    @Override
    protected void doWriteStartArray() {
        buffer.write(BsonType.ARRAY.getValue());
        writeCurrentName();
        setContext(new Context(getContext(), BsonContextType.ARRAY, buffer.getPosition()));
        buffer.writeInt(0); // reserve space for size
    }

    @Override
    protected void doWriteEndArray() {
        buffer.write(0);
        backpatchSize(); // size of document
        setContext(getContext().getParentContext());
    }

    @Override
    protected void doWriteBinaryData(final BsonBinary value) {
        buffer.write(BsonType.BINARY.getValue());
        writeCurrentName();

        int totalLen = value.getData().length;

        if (value.getType() == BsonBinarySubType.OLD_BINARY.getValue()) {
            totalLen += 4;
        }

        buffer.writeInt(totalLen);
        buffer.write(value.getType());
        if (value.getType() == BsonBinarySubType.OLD_BINARY.getValue()) {
            buffer.writeInt(totalLen - 4);
        }
        buffer.write(value.getData());
    }

    @Override
    public void doWriteBoolean(final boolean value) {
        buffer.write(BsonType.BOOLEAN.getValue());
        writeCurrentName();
        buffer.write(value ? 1 : 0);
    }

    @Override
    protected void doWriteDateTime(final long value) {
        buffer.write(BsonType.DATE_TIME.getValue());
        writeCurrentName();
        buffer.writeLong(value);
    }

    @Override
    protected void doWriteDBPointer(final BsonDbPointer value) {
        buffer.write(BsonType.DB_POINTER.getValue());
        writeCurrentName();

        buffer.writeString(value.getNamespace());
        buffer.write(value.getId().toByteArray());
    }

    @Override
    protected void doWriteDouble(final double value) {
        buffer.write(BsonType.DOUBLE.getValue());
        writeCurrentName();
        buffer.writeDouble(value);
    }

    @Override
    protected void doWriteInt32(final int value) {
        buffer.write(BsonType.INT32.getValue());
        writeCurrentName();
        buffer.writeInt(value);
    }

    @Override
    protected void doWriteInt64(final long value) {
        buffer.write(BsonType.INT64.getValue());
        writeCurrentName();
        buffer.writeLong(value);
    }

    @Override
    protected void doWriteJavaScript(final String value) {
        buffer.write(BsonType.JAVASCRIPT.getValue());
        writeCurrentName();
        buffer.writeString(value);
    }

    @Override
    protected void doWriteJavaScriptWithScope(final String value) {
        buffer.write(BsonType.JAVASCRIPT_WITH_SCOPE.getValue());
        writeCurrentName();
        setContext(new Context(getContext(), BsonContextType.JAVASCRIPT_WITH_SCOPE, buffer.getPosition()));
        buffer.writeInt(0);
        buffer.writeString(value);
    }

    @Override
    protected void doWriteMaxKey() {
        buffer.write(BsonType.MAX_KEY.getValue());
        writeCurrentName();
    }

    @Override
    protected void doWriteMinKey() {
        buffer.write(BsonType.MIN_KEY.getValue());
        writeCurrentName();
    }

    @Override
    public void doWriteNull() {
        buffer.write(BsonType.NULL.getValue());
        writeCurrentName();
    }

    @Override
    public void doWriteObjectId(final ObjectId value) {
        buffer.write(BsonType.OBJECT_ID.getValue());
        writeCurrentName();
        buffer.write(value.toByteArray());
    }

    @Override
    public void doWriteRegularExpression(final BsonRegularExpression value) {
        buffer.write(BsonType.REGULAR_EXPRESSION.getValue());
        writeCurrentName();
        buffer.writeCString(value.getPattern());
        buffer.writeCString(value.getOptions());
    }

    @Override
    public void doWriteString(final String value) {
        buffer.write(BsonType.STRING.getValue());
        writeCurrentName();
        buffer.writeString(value);
    }

    @Override
    public void doWriteSymbol(final String value) {
        buffer.write(BsonType.SYMBOL.getValue());
        writeCurrentName();
        buffer.writeString(value);
    }

    @Override
    public void doWriteTimestamp(final BsonTimestamp value) {
        buffer.write(BsonType.TIMESTAMP.getValue());
        writeCurrentName();
        buffer.writeInt(value.getInc());
        buffer.writeInt(value.getTime());
    }

    @Override
    public void doWriteUndefined() {
        buffer.write(BsonType.UNDEFINED.getValue());
        writeCurrentName();
    }

    @Override
    public void pipe(final BsonReader reader) {
        if (reader instanceof BsonBinaryReader) {
            BsonBinaryReader binaryReader = (BsonBinaryReader) reader;
            if (getState() == State.VALUE) {
                buffer.write(BsonType.DOCUMENT.getValue());
                writeCurrentName();
            }
            BsonInputStream bsonInputStream = binaryReader.getBuffer();
            int size = bsonInputStream.readInt32();
            buffer.writeInt(size);
            byte[] bytes = new byte[size - 4];
            bsonInputStream.readBytes(bytes);
            buffer.write(bytes);
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
