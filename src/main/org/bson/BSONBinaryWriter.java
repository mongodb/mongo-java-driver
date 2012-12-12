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

import org.bson.io.OutputBuffer;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;

import static org.bson.BSON.B_BINARY;

public class BSONBinaryWriter extends BSONWriter {

    private final OutputBuffer buffer;
    private Context context;
    private BinaryWriterSettings binaryWriterSettings;

    public BSONBinaryWriter(BsonWriterSettings settings, BinaryWriterSettings binaryWriterSettings, OutputBuffer buffer) {
        super(settings);
        this.binaryWriterSettings = binaryWriterSettings;
        this.buffer = buffer;
    }

    @Override
    public void flush() {
    }

    @Override
    public void writeBinaryData(final Binary binary) {
        checkPreconditions("writeBinaryData", State.VALUE);

        buffer.write(BSON.BINARY);
        writeCurrentName();

        int totalLen = binary.length();

        if (binary.getType() == B_BINARY) {
            totalLen += 4;
        }

        buffer.writeInt(totalLen);
        buffer.write(binary.getType());
        if (binary.getType() == B_BINARY) {
            buffer.writeInt(totalLen - 4);
        }
        buffer.write(binary.getData());

        setState(getNextState());
    }

    @Override
    public void writeBoolean(final boolean value) {
        checkPreconditions("writeBoolean", State.VALUE);

        buffer.write(BSON.BOOLEAN);
        writeCurrentName();
        buffer.write(value ? 1 : 0);

        setState(getNextState());
    }

    @Override
    public void writeDateTime(final long value) {
        checkPreconditions("writeDateTime", State.VALUE);

        buffer.write(BSON.DATE);
        writeCurrentName();
        buffer.writeLong(value);

        setState(getNextState());
    }

    @Override
    public void writeDouble(final double value) {
        checkPreconditions("writeDouble", State.VALUE);

        buffer.write(BSON.NUMBER);
        writeCurrentName();
        buffer.writeDouble(value);

        setState(getNextState());
    }

    @Override
    public void writeInt32(final int value) {
        checkPreconditions("writeInt32", State.VALUE);

        buffer.write(BSON.NUMBER_INT);
        writeCurrentName();
        buffer.writeInt(value);

        setState(getNextState());
    }

    @Override
    public void writeInt64(final long value) {
        checkPreconditions("writeInt64", State.VALUE);

        buffer.write(BSON.NUMBER_LONG);
        writeCurrentName();
        buffer.writeLong(value);

        setState(getNextState());
    }

    @Override
    public void writeJavaScript(final String code) {
        checkPreconditions("writeJavaScript", State.VALUE);

        buffer.write(BSON.CODE);
        writeCurrentName();
        buffer.writeString(code);

        setState(getNextState());
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        checkPreconditions("writeJavaScriptWithScope", State.VALUE);

        buffer.write(BSON.CODE_W_SCOPE);
        writeCurrentName();
        context = new Context(context, ContextType.JAVASCRIPT_WITH_SCOPE, buffer.getPosition());
        buffer.writeInt(0);
        buffer.writeString(code);

        setState(State.SCOPE_DOCUMENT);
    }

    @Override
    public void writeMaxKey() {
        checkPreconditions("writeMaxKey", State.VALUE);

        buffer.write(BSON.MAXKEY);
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void writeMinKey() {
        checkPreconditions("writeMinKey", State.VALUE);

        buffer.write(BSON.MINKEY);
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void writeNull() {
        checkPreconditions("writeNull", State.VALUE);

        buffer.write(BSON.NULL);
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void writeObjectId(ObjectId objectId) {
        checkPreconditions("writeObjectId", State.VALUE);

        buffer.write(BSON.OID);
        writeCurrentName();

        // TODO: Should this be pushed down into the buffer?
        buffer.writeIntBE(objectId._time());
        buffer.writeIntBE(objectId._machine());
        buffer.writeIntBE(objectId._inc());

        setState(getNextState());
    }

    @Override
    public void writeRegularExpression(RegularExpression regularExpression) {
        checkPreconditions("writeRegularExpression", State.VALUE);

        buffer.write(BSON.REGEX);
        writeCurrentName();
        buffer.writeCString(regularExpression.getPattern());
        buffer.writeCString(regularExpression.getOptions());

        setState(getNextState());
    }

    @Override
    public void writeString(final String value) {
        checkPreconditions("writeString", State.VALUE);

        buffer.write(BSON.STRING);
        writeCurrentName();
        buffer.writeString(value);

        setState(getNextState());
    }

    @Override
    public void writeSymbol(final String value) {
        checkPreconditions("writeSymbol", State.VALUE);

        buffer.write(BSON.SYMBOL);
        writeCurrentName();
        buffer.writeString(value);

        setState(getNextState());
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void writeTimestamp(final long value) {
        checkPreconditions("writeTimestamp", State.VALUE);

        buffer.write(BSON.TIMESTAMP);
        writeCurrentName();
        buffer.writeLong(value);

        setState(getNextState());
    }

    @Override
    public void writeUndefined() {
        checkPreconditions("writeUndefined", State.VALUE);

        buffer.write(BSON.UNDEFINED);
        writeCurrentName();

        setState(getNextState());
    }

    @Override
    public void close() {
    }

    /// <summary>
    /// Writes the start of a BSON array to the writer.
    /// </summary>
    @Override
    public void writeStartArray() {
        checkPreconditions("writeStartArray", State.VALUE);

        super.writeStartArray();
        buffer.write(BSON.ARRAY);
        writeCurrentName();
        context = new Context(context, ContextType.ARRAY, buffer.getPosition());
        buffer.writeInt(0); // reserve space for size

        setState(State.VALUE);
    }

    /// <summary>
    /// Writes the start of a BSON document to the writer.
    /// </summary>
    @Override
    public void writeStartDocument() {
        checkPreconditions("writeStartDocument", State.INITIAL, State.VALUE, State.SCOPE_DOCUMENT, State.DONE);

        super.writeStartDocument();
        if (getState() == State.VALUE) {
            buffer.write(BSON.OBJECT);
            writeCurrentName();
        }
        ContextType contextType = (getState() == State.SCOPE_DOCUMENT) ? ContextType.SCOPE_DOCUMENT : ContextType.DOCUMENT;
        context = new Context(context, ContextType.DOCUMENT, buffer.getPosition());
        buffer.writeInt(0); // reserve space for size

        setState(State.NAME);
    }

    /// <summary>
    /// Writes the end of a BSON array to the writer.
    /// </summary>
    @Override
    public void writeEndArray() {
        checkPreconditions("writeEndArray", State.VALUE);

        if (context.contextType != ContextType.ARRAY) {
            throwInvalidContextType("WriteEndArray", context.contextType, ContextType.ARRAY);
        }

        super.writeEndArray();
        buffer.write(0);
        backpatchSize(); // size of document

        context = context.parentContext;
        setState(getNextState());
    }

    /// <summary>
    /// Writes the end of a BSON document to the writer.
    /// </summary>
    @Override
    public void writeEndDocument() {
        checkPreconditions("writeEndDocument", State.NAME);

        if (context.contextType != ContextType.DOCUMENT && context.contextType != ContextType.SCOPE_DOCUMENT) {
            throwInvalidContextType("WriteEndDocument", context.contextType, ContextType.DOCUMENT, ContextType.SCOPE_DOCUMENT);
        }

        super.writeEndDocument();
        buffer.write(0);
        backpatchSize(); // size of document

        context = context.parentContext;
        if (context == null) {
            setState(State.DONE);
        }
        else {
            if (context.contextType == ContextType.JAVASCRIPT_WITH_SCOPE) {
                backpatchSize(); // size of the JavaScript with scope value
                context = context.parentContext;
            }
            setState(getNextState());
        }
    }

    private void writeCurrentName() {
        if (context.contextType == ContextType.ARRAY) {
            buffer.writeCString(Integer.toString(context.index++));
        }
        else {
            buffer.writeCString(getName());
        }
    }

    private State getNextState() {
        if (context.contextType == ContextType.ARRAY) {
            return State.VALUE;
        }
        else {
            return State.NAME;
        }
    }

    private void checkPreconditions(String methodName, final State... validStates) {
        if (isClosed()) {
            throw new IllegalStateException("BsonBinaryWriter");
        }

        if (!checkState(validStates)) {
            throwInvalidState(methodName, validStates);
        }
    }

    private boolean checkState(final State[] validStates) {
        for (State cur : validStates) {
            if (cur == getState()) {
                return true;
            }
        }
        return false;
    }

    private void backpatchSize() {
        int size = buffer.getPosition() - context.startPosition;
        if (size > binaryWriterSettings.maxDocumentSize) {
            String message = String.format("Size %d is larger than MaxDocumentSize %d.", size, binaryWriterSettings.maxDocumentSize);
            throw new BsonSerializationException(message);
        }
        buffer.backpatchSize(size);
    }

    private static class Context {
        // private fields
        private final Context parentContext;
        private final ContextType contextType;
        private final int startPosition;
        private int index; // used when contextType is an array

        Context(Context parentContext, ContextType contextType, int startPosition) {
            this.parentContext = parentContext;
            this.contextType = contextType;
            this.startPosition = startPosition;
        }
    }
}
