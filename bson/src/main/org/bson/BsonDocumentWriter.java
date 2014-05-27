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

import org.bson.types.Binary;
import org.bson.types.BsonArray;
import org.bson.types.BsonBoolean;
import org.bson.types.BsonDateTime;
import org.bson.types.BsonDocument;
import org.bson.types.BsonDouble;
import org.bson.types.BsonInt32;
import org.bson.types.BsonInt64;
import org.bson.types.BsonNull;
import org.bson.types.BsonString;
import org.bson.types.BsonValue;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.bson.types.Symbol;
import org.bson.types.Timestamp;
import org.bson.types.Undefined;

import static org.bson.BSONContextType.DOCUMENT;
import static org.bson.BSONContextType.SCOPE_DOCUMENT;

/**
 * Writes into an instance of BsonDocument.
 *
 * @since 3.0
 */
public class BsonDocumentWriter extends AbstractBSONWriter {

    private final BsonDocument document;

    public BsonDocumentWriter(final BsonDocument document) {
        super(new BSONWriterSettings());
        this.document = document;
        setContext(new Context());
    }

    @Override
    public void writeBinaryData(final Binary binary) {
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
        write(new Code(code));
        setState(getNextState());
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        checkPreconditions("writeJavaScriptWithScope", State.VALUE);

        setContext(new Context(new BsonString(code), BSONContextType.JAVASCRIPT_WITH_SCOPE, getContext()));

        setState(State.SCOPE_DOCUMENT);
    }

    @Override
    public void writeMaxKey() {
        checkPreconditions("writeMaxKey", State.VALUE);
        write(new MaxKey());
        setState(getNextState());
    }

    @Override
    public void writeMinKey() {
        checkPreconditions("writeMinKey", State.VALUE);
        write(new MinKey());
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
        write(objectId);
        setState(getNextState());
    }

    @Override
    public void writeRegularExpression(final RegularExpression regularExpression) {
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
        write(new Symbol(value));
        setState(getNextState());
    }

    @Override
    public void writeTimestamp(final Timestamp value) {
        checkPreconditions("writeTimestamp", State.VALUE);
        write(value);
        setState(getNextState());
    }

    @Override
    public void writeUndefined() {
        checkPreconditions("writeUndefined", State.VALUE);
        write(new Undefined());
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
                throw new BSONInvalidOperationException("Unexpected state " + getState());
        }

        setState(State.NAME);
    }

    @Override
    public void writeEndDocument() {
        checkPreconditions("writeEndDocument", State.NAME);

        super.writeEndDocument();

        BsonValue value = getContext().container;
        setContext(getContext().getParentContext());

        if (getContext().getContextType() == BSONContextType.JAVASCRIPT_WITH_SCOPE) {
            BsonDocument scope = (BsonDocument) value;
            BsonString code = (BsonString) getContext().container;
            setContext(getContext().getParentContext());
            write(new CodeWithScope(code.getValue(), scope));
        } else {
            if (getContext().getContextType() == BSONContextType.TOP_LEVEL) {
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

        setContext(new Context(new BsonArray(), BSONContextType.ARRAY, getContext()));
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

    private class Context extends AbstractBSONWriter.Context {
        private BsonValue container;

        public Context(final BsonValue container, final BSONContextType contextType, final Context parent) {
            super(parent, contextType);
            this.container = container;
        }

        public Context() {
            super(null, BSONContextType.TOP_LEVEL);
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
