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

import org.bson.io.BsonInput;
import org.bson.io.BsonOutput;
import org.bson.types.ObjectId;

import java.util.Stack;

/**
 * A BsonWriter implementation that writes to a binary stream of data.  This is the most commonly used implementation.
 *
 * @since 3.0
 */
public class BsonBinaryWriter extends AbstractBsonWriter {
    private final BsonBinaryWriterSettings binaryWriterSettings;

    private final BsonOutput bsonOutput;
    private final boolean closeOutput;
    private final Stack<Integer> maxDocumentSizeStack = new Stack<Integer>();
    private Mark mark;

    /**
     * Construct an instance.
     *
     * @param bsonOutput the output to write to
     * @param validator the field name validator to apply
     */
    public BsonBinaryWriter(final BsonOutput bsonOutput, final FieldNameValidator validator) {
        this(new BsonWriterSettings(), new BsonBinaryWriterSettings(), bsonOutput, validator);
    }

    /**
     * Construct an instance.
     *
     * @param bsonOutput the output to write to
     * @param closeOutput whether to close the bsonOutput when it is closed itself
     */
    public BsonBinaryWriter(final BsonOutput bsonOutput, final boolean closeOutput) {
        this(new BsonWriterSettings(), new BsonBinaryWriterSettings(), bsonOutput, closeOutput);
    }

    /**
     * Construct an instance.
     *
     * @param settings the generic BsonWriter settings
     * @param binaryWriterSettings the settings specific to a BsonBinaryWriter
     * @param bsonOutput the output to write to
     * @param closeOutput whether to close the bsonOutput when it is closed itself
     */
    public BsonBinaryWriter(final BsonWriterSettings settings, final BsonBinaryWriterSettings binaryWriterSettings,
                            final BsonOutput bsonOutput, final boolean closeOutput) {
        this(settings, binaryWriterSettings, bsonOutput, new NoOpFieldNameValidator(), closeOutput);
    }

    /**
     * Construct an instance.
     *
     * @param settings the generic BsonWriter settings
     * @param binaryWriterSettings the settings specific to a BsonBinaryWriter
     * @param bsonOutput the output to write to
     * @param validator the field name validator to apply
     */
    public BsonBinaryWriter(final BsonWriterSettings settings, final BsonBinaryWriterSettings binaryWriterSettings,
                            final BsonOutput bsonOutput, final FieldNameValidator validator) {
        this(settings, binaryWriterSettings, bsonOutput, validator, false);
    }

    private BsonBinaryWriter(final BsonWriterSettings settings, final BsonBinaryWriterSettings binaryWriterSettings,
                             final BsonOutput bsonOutput, final FieldNameValidator validator, final boolean closeOutput) {
        super(settings, validator);
        this.binaryWriterSettings = binaryWriterSettings;
        this.bsonOutput = bsonOutput;
        this.closeOutput = closeOutput;
        maxDocumentSizeStack.push(binaryWriterSettings.getMaxDocumentSize());
    }

    @Override
    public void close() {
        super.close();
        if (closeOutput) {
            bsonOutput.close();
        }
    }

    /**
     * Gets the BSON output backing this instance.
     *
     * @return the BSON output
     */
    public BsonOutput getBsonOutput() {
        return bsonOutput;
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
            bsonOutput.writeByte(BsonType.DOCUMENT.getValue());
            writeCurrentName();
        }
        setContext(new Context(getContext(), BsonContextType.DOCUMENT, bsonOutput.getPosition()));
        bsonOutput.writeInt32(0); // reserve space for size
    }

    @Override
    protected void doWriteEndDocument() {
        bsonOutput.writeByte(0);
        backpatchSize(); // size of document

        setContext(getContext().getParentContext());
        if (getContext() != null && getContext().getContextType() == BsonContextType.JAVASCRIPT_WITH_SCOPE) {
            backpatchSize(); // size of the JavaScript with scope value
            setContext(getContext().getParentContext());
        }
    }

    @Override
    protected void doWriteStartArray() {
        bsonOutput.writeByte(BsonType.ARRAY.getValue());
        writeCurrentName();
        setContext(new Context(getContext(), BsonContextType.ARRAY, bsonOutput.getPosition()));
        bsonOutput.writeInt32(0); // reserve space for size
    }

    @Override
    protected void doWriteEndArray() {
        bsonOutput.writeByte(0);
        backpatchSize(); // size of document
        setContext(getContext().getParentContext());
    }

    @Override
    protected void doWriteBinaryData(final BsonBinary value) {
        bsonOutput.writeByte(BsonType.BINARY.getValue());
        writeCurrentName();

        int totalLen = value.getData().length;

        if (value.getType() == BsonBinarySubType.OLD_BINARY.getValue()) {
            totalLen += 4;
        }

        bsonOutput.writeInt32(totalLen);
        bsonOutput.writeByte(value.getType());
        if (value.getType() == BsonBinarySubType.OLD_BINARY.getValue()) {
            bsonOutput.writeInt32(totalLen - 4);
        }
        bsonOutput.writeBytes(value.getData());
    }

    @Override
    public void doWriteBoolean(final boolean value) {
        bsonOutput.writeByte(BsonType.BOOLEAN.getValue());
        writeCurrentName();
        bsonOutput.writeByte(value ? 1 : 0);
    }

    @Override
    protected void doWriteDateTime(final long value) {
        bsonOutput.writeByte(BsonType.DATE_TIME.getValue());
        writeCurrentName();
        bsonOutput.writeInt64(value);
    }

    @Override
    protected void doWriteDBPointer(final BsonDbPointer value) {
        bsonOutput.writeByte(BsonType.DB_POINTER.getValue());
        writeCurrentName();

        bsonOutput.writeString(value.getNamespace());
        bsonOutput.writeBytes(value.getId().toByteArray());
    }

    @Override
    protected void doWriteDouble(final double value) {
        bsonOutput.writeByte(BsonType.DOUBLE.getValue());
        writeCurrentName();
        bsonOutput.writeDouble(value);
    }

    @Override
    protected void doWriteInt32(final int value) {
        bsonOutput.writeByte(BsonType.INT32.getValue());
        writeCurrentName();
        bsonOutput.writeInt32(value);
    }

    @Override
    protected void doWriteInt64(final long value) {
        bsonOutput.writeByte(BsonType.INT64.getValue());
        writeCurrentName();
        bsonOutput.writeInt64(value);
    }

    @Override
    protected void doWriteJavaScript(final String value) {
        bsonOutput.writeByte(BsonType.JAVASCRIPT.getValue());
        writeCurrentName();
        bsonOutput.writeString(value);
    }

    @Override
    protected void doWriteJavaScriptWithScope(final String value) {
        bsonOutput.writeByte(BsonType.JAVASCRIPT_WITH_SCOPE.getValue());
        writeCurrentName();
        setContext(new Context(getContext(), BsonContextType.JAVASCRIPT_WITH_SCOPE, bsonOutput.getPosition()));
        bsonOutput.writeInt32(0);
        bsonOutput.writeString(value);
    }

    @Override
    protected void doWriteMaxKey() {
        bsonOutput.writeByte(BsonType.MAX_KEY.getValue());
        writeCurrentName();
    }

    @Override
    protected void doWriteMinKey() {
        bsonOutput.writeByte(BsonType.MIN_KEY.getValue());
        writeCurrentName();
    }

    @Override
    public void doWriteNull() {
        bsonOutput.writeByte(BsonType.NULL.getValue());
        writeCurrentName();
    }

    @Override
    public void doWriteObjectId(final ObjectId value) {
        bsonOutput.writeByte(BsonType.OBJECT_ID.getValue());
        writeCurrentName();
        bsonOutput.writeBytes(value.toByteArray());
    }

    @Override
    public void doWriteRegularExpression(final BsonRegularExpression value) {
        bsonOutput.writeByte(BsonType.REGULAR_EXPRESSION.getValue());
        writeCurrentName();
        bsonOutput.writeCString(value.getPattern());
        bsonOutput.writeCString(value.getOptions());
    }

    @Override
    public void doWriteString(final String value) {
        bsonOutput.writeByte(BsonType.STRING.getValue());
        writeCurrentName();
        bsonOutput.writeString(value);
    }

    @Override
    public void doWriteSymbol(final String value) {
        bsonOutput.writeByte(BsonType.SYMBOL.getValue());
        writeCurrentName();
        bsonOutput.writeString(value);
    }

    @Override
    public void doWriteTimestamp(final BsonTimestamp value) {
        bsonOutput.writeByte(BsonType.TIMESTAMP.getValue());
        writeCurrentName();
        bsonOutput.writeInt32(value.getInc());
        bsonOutput.writeInt32(value.getTime());
    }

    @Override
    public void doWriteUndefined() {
        bsonOutput.writeByte(BsonType.UNDEFINED.getValue());
        writeCurrentName();
    }

    @Override
    public void pipe(final BsonReader reader) {
        if (reader instanceof BsonBinaryReader) {
            BsonBinaryReader binaryReader = (BsonBinaryReader) reader;
            if (getState() == State.VALUE) {
                bsonOutput.writeByte(BsonType.DOCUMENT.getValue());
                writeCurrentName();
            }
            BsonInput bsonInput = binaryReader.getBsonInput();
            int size = bsonInput.readInt32();
            bsonOutput.writeInt32(size);
            byte[] bytes = new byte[size - 4];
            bsonInput.readBytes(bytes);
            bsonOutput.writeBytes(bytes);
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
            bsonOutput.writeCString(Integer.toString(getContext().index++));
        } else {
            bsonOutput.writeCString(getName());
        }
    }

    private void backpatchSize() {
        int size = bsonOutput.getPosition() - getContext().startPosition;
        if (size > maxDocumentSizeStack.peek()) {
            String message = String.format("Size %d is larger than MaxDocumentSize %d.", size,
                                           binaryWriterSettings.getMaxDocumentSize());
            throw new BsonSerializationException(message);
        }
        bsonOutput.writeInt32(bsonOutput.getPosition() - size, size);
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
            this.position = bsonOutput.getPosition();
        }

        protected void reset() {
            super.reset();
            bsonOutput.truncateToPosition(mark.position);
        }
    }
}
