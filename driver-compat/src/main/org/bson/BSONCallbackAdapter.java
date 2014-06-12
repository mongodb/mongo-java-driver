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
import org.bson.types.BsonDbPointer;
import org.bson.types.BsonRegularExpression;
import org.bson.types.ObjectId;

import static org.bson.io.Bits.readLong;

class BsonCallbackAdapter extends AbstractBsonWriter {

    private BSONCallback bsonCallback;

    /**
     * Initializes a new instance of the BsonWriter class.
     *
     * @param settings     The writer settings.
     * @param bsonCallback The callback to inform of operations on this writer
     */
    protected BsonCallbackAdapter(final BsonWriterSettings settings, final BSONCallback bsonCallback) {
        super(settings);
        this.bsonCallback = bsonCallback;
    }

    @Override
    public void flush() {
        //Looks like should be no-op?
    }

    @Override
    public void writeStartArray() {
        super.writeStartArray();
        bsonCallback.arrayStart(getName());
        setContext(new Context(getContext(), BsonContextType.ARRAY));
        setState(State.VALUE);
    }

    @Override
    public void writeStartDocument() {
        super.writeStartDocument();
        BsonContextType contextType = getState() == State.SCOPE_DOCUMENT
                                      ? BsonContextType.SCOPE_DOCUMENT
                                      : BsonContextType.DOCUMENT;

        if (getContext() == null || contextType == BsonContextType.SCOPE_DOCUMENT) {
            bsonCallback.objectStart();
        } else {
            bsonCallback.objectStart(getName());
        }
        setContext(new Context(getContext(), contextType));
        setState(State.NAME);
    }

    @Override
    public void writeEndArray() {
        super.writeEndArray();
        if (getContext().getContextType() != BsonContextType.ARRAY) {
            throwInvalidContextType("WriteEndArray", getContext().getContextType(), BsonContextType.ARRAY);
        }
        setContext(getContext().getParentContext());
        setState(getNextState());
        bsonCallback.arrayDone();
    }

    @Override
    public void writeEndDocument() {
        super.writeEndDocument();
        BsonContextType contextType = getContext().getContextType();

        if (contextType != BsonContextType.DOCUMENT && contextType != BsonContextType.SCOPE_DOCUMENT) {
            throwInvalidContextType("WriteEndDocument", contextType, BsonContextType.DOCUMENT, BsonContextType.SCOPE_DOCUMENT);
        }

        setContext(getContext().getParentContext());
        bsonCallback.objectDone();

        if (contextType == BsonContextType.SCOPE_DOCUMENT) {
            Object scope = bsonCallback.get();
            bsonCallback = getContext().callback;
            bsonCallback.gotCodeWScope(getContext().name, getContext().code, scope);
        }
    }

    @Override
    public void writeBinaryData(final Binary binary) {
        if (binary.getType() == BsonBinarySubType.UUID_LEGACY.getValue()) {
            bsonCallback.gotUUID(getName(),
                                 readLong(binary.getData(), 0),
                                 readLong(binary.getData(), 8));
        } else {
            bsonCallback.gotBinary(getName(), binary.getType(), binary.getData());
        }

        setState(getNextState());
    }

    @Override
    public void writeBoolean(final boolean value) {
        bsonCallback.gotBoolean(getName(), value);
        setState(getNextState());
    }

    @Override
    public void writeDateTime(final long value) {
        bsonCallback.gotDate(getName(), value);
        setState(getNextState());
    }

    @Override
    public void writeDouble(final double value) {
        bsonCallback.gotDouble(getName(), value);
        setState(getNextState());
    }

    @Override
    public void writeInt32(final int value) {
        bsonCallback.gotInt(getName(), value);
        setState(getNextState());
    }

    @Override
    public void writeInt64(final long value) {
        bsonCallback.gotLong(getName(), value);
        setState(getNextState());
    }

    @Override
    public void writeJavaScript(final String code) {
        bsonCallback.gotCode(getName(), code);
        setState(getNextState());
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        getContext().callback = bsonCallback;
        getContext().code = code;
        getContext().name = getName();
        this.bsonCallback = bsonCallback.createBSONCallback();
        setState(State.SCOPE_DOCUMENT);
    }

    @Override
    public void writeMaxKey() {
        bsonCallback.gotMaxKey(getName());
        setState(getNextState());
    }

    @Override
    public void writeMinKey() {
        bsonCallback.gotMinKey(getName());
        setState(getNextState());
    }

    @Override
    public void writeNull() {
        bsonCallback.gotNull(getName());
        setState(getNextState());
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        bsonCallback.gotObjectId(getName(), objectId);
        setState(getNextState());
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        bsonCallback.gotRegex(getName(), regularExpression.getPattern(), regularExpression.getOptions());
        setState(getNextState());
    }

    @Override
    public void writeString(final String value) {
        bsonCallback.gotString(getName(), value);
        setState(getNextState());
    }

    @Override
    public void writeSymbol(final String value) {
        bsonCallback.gotSymbol(getName(), value);
        setState(getNextState());
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        bsonCallback.gotTimestamp(getName(), value.getTime(), value.getInc());
        setState(getNextState());
    }

    @Override
    public void writeUndefined() {
        bsonCallback.gotUndefined(getName());
        setState(getNextState());
    }

    @Override
    public void writeDBPointer(final BsonDbPointer value) {
        bsonCallback.gotDBRef(getName(), value.getNamespace(), value.getId());
    }

    @Override
    protected Context getContext() {
        return (Context) super.getContext();
    }

    @Override
    protected String getName() {
        if (getContext().getContextType() == BsonContextType.ARRAY) {
            return Integer.toString(getContext().index++);
        } else {
            return super.getName();
        }
    }

    public class Context extends AbstractBsonWriter.Context {
        private int index; // used when contextType is an array
        private BSONCallback callback;
        private String code;
        private String name;

        public Context(final Context parentContext, final BsonContextType contextType) {
            super(parentContext, contextType);
        }

        @Override
        public Context getParentContext() {
            return (Context) super.getParentContext();
        }
    }
}
