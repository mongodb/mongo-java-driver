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

import org.bson.types.ObjectId;

import static org.bson.BsonContextType.DOCUMENT;
import static org.bson.BsonContextType.SCOPE_DOCUMENT;

/**
 * A {@code BsonWriter} implementation that writes to an instance of {@code BsonDocument}.  This can be used to encode an object into a
 * {@code BsonDocument} using an {@code Encoder}.
 *
 * @see BsonDocument
 * @see org.bson.codecs.Encoder
 *
 * @since 3.0
 */
public class BsonDocumentWriter extends AbstractBsonWriter {

    private final BsonDocument document;

    /**
     * Construct a new instance.
     *
     * @param document the document to write to
     */
    public BsonDocumentWriter(final BsonDocument document) {
        super(new BsonWriterSettings());
        this.document = document;
        setContext(new Context());
    }

    @Override
    public void writeBinaryData(final BsonBinary binary) {
        checkPreconditions("writeBinaryData", State.VALUE);
        write(binary);
        setState(getNextState());
    }

    @Override
    public void writeBoolean(final boolean value) {
        checkPreconditions("writeBoolean", State.VALUE);
        write(BsonBoolean.valueOf(value));
        setState(getNextState());
    }

    @Override
    public void writeDateTime(final long value) {
        checkPreconditions("writeDateTime", State.VALUE);
        write(new BsonDateTime(value));
        setState(getNextState());
    }

    @Override
    public void writeDouble(final double value) {
        checkPreconditions("writeDouble", State.VALUE);
        write(new BsonDouble(value));
        setState(getNextState());
    }

    @Override
    public void writeInt32(final int value) {
        checkPreconditions("writeInt32", State.VALUE);
        write(new BsonInt32(value));
        setState(getNextState());
    }

    @Override
    public void writeInt64(final long value) {
        checkPreconditions("", State.VALUE);
        write(new BsonInt64(value));
        setState(getNextState());
    }

    @Override
    public void writeJavaScript(final String code) {
        checkPreconditions("writeInt64", State.VALUE);
        write(new BsonJavaScript(code));
        setState(getNextState());
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        checkPreconditions("writeJavaScriptWithScope", State.VALUE);

        setContext(new Context(new BsonString(code), BsonContextType.JAVASCRIPT_WITH_SCOPE, getContext()));

        setState(State.SCOPE_DOCUMENT);
    }

    @Override
    public void writeMaxKey() {
        checkPreconditions("writeMaxKey", State.VALUE);
        write(new BsonMaxKey());
        setState(getNextState());
    }

    @Override
    public void writeMinKey() {
        checkPreconditions("writeMinKey", State.VALUE);
        write(new BsonMinKey());
        setState(getNextState());
    }

    @Override
    public void writeNull() {
        checkPreconditions("writeNull", State.VALUE);
        write(BsonNull.VALUE);
        setState(getNextState());
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        checkPreconditions("writeObjectId", State.VALUE);
        write(new BsonObjectId(objectId));
        setState(getNextState());
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        checkPreconditions("writeRegularExpression", State.VALUE);
        write(regularExpression);
        setState(getNextState());
    }

    @Override
    public void writeString(final String value) {
        checkPreconditions("writeString", State.VALUE);
        write(new BsonString(value));
        setState(getNextState());
    }

    @Override
    public void writeSymbol(final String value) {
        checkPreconditions("writeSymbol", State.VALUE);
        write(new BsonSymbol(value));
        setState(getNextState());
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        checkPreconditions("writeTimestamp", State.VALUE);
        write(value);
        setState(getNextState());
    }

    @Override
    public void writeUndefined() {
        checkPreconditions("writeUndefined", State.VALUE);
        write(new BsonUndefined());
        setState(getNextState());
    }

    @Override
    public void writeDBPointer(final BsonDbPointer value) {
        checkPreconditions("writeDBPointer", State.VALUE);
        write(value);
        setState(getNextState());
    }

    @Override
    public void writeStartDocument() {
        checkPreconditions("writeStartDocument", State.INITIAL, State.VALUE, State.SCOPE_DOCUMENT, State.DONE);

        super.writeStartDocument();
        switch (getState()) {
            case INITIAL:
                setContext(new Context(document, DOCUMENT, getContext()));
                break;
            case VALUE:
                setContext(new Context(new BsonDocument(), DOCUMENT, getContext()));
                break;
            case SCOPE_DOCUMENT:
                setContext(new Context(new BsonDocument(), SCOPE_DOCUMENT, getContext()));
                break;
            default:
                throw new BsonInvalidOperationException("Unexpected state " + getState());
        }

        setState(State.NAME);
    }

    @Override
    public void writeEndDocument() {
        checkPreconditions("writeEndDocument", State.NAME);

        super.writeEndDocument();

        BsonValue value = getContext().container;
        setContext(getContext().getParentContext());

        if (getContext().getContextType() == BsonContextType.JAVASCRIPT_WITH_SCOPE) {
            BsonDocument scope = (BsonDocument) value;
            BsonString code = (BsonString) getContext().container;
            setContext(getContext().getParentContext());
            write(new BsonJavaScriptWithScope(code.getValue(), scope));
        } else {
            if (getContext().getContextType() == BsonContextType.TOP_LEVEL) {
                setState(State.DONE);
            } else {
                write(value);
                setState(getNextState());
            }
        }
    }

    @Override
    public void writeStartArray() {
        checkPreconditions("writeStartArray", State.VALUE);
        super.writeStartArray();

        setContext(new Context(new BsonArray(), BsonContextType.ARRAY, getContext()));
        setState(State.VALUE);
    }

    @Override
    public void writeEndArray() {
        checkPreconditions("writeEndArray", State.VALUE);
        super.writeEndArray();

        BsonValue array = getContext().container;
        setContext(getContext().getParentContext());
        write(array);

        setState(getNextState());
    }

    @Override
    public void flush() {
    }

    @Override
    protected Context getContext() {
        return (Context) super.getContext();
    }

    private void write(final BsonValue value) {
        getContext().add(value);
    }

    private class Context extends AbstractBsonWriter.Context {
        private BsonValue container;

        public Context(final BsonValue container, final BsonContextType contextType, final Context parent) {
            super(parent, contextType);
            this.container = container;
        }

        public Context() {
            super(null, BsonContextType.TOP_LEVEL);
        }

        void add(final BsonValue value) {
            if (container instanceof BsonArray) {
                ((BsonArray) container).add(value);
            } else {
                ((BsonDocument) container).put(getName(), value);
            }
        }
    }
}
