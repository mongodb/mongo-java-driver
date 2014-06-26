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
    public void doWriteStartDocument() {
        BsonContextType contextType = getState() == State.SCOPE_DOCUMENT
                ? BsonContextType.SCOPE_DOCUMENT
                : BsonContextType.DOCUMENT;

        if (getContext() == null || contextType == BsonContextType.SCOPE_DOCUMENT) {
            bsonCallback.objectStart();
        } else {
            bsonCallback.objectStart(getName());
        }
        setContext(new Context(getContext(), contextType));
    }

    @Override
    protected void doWriteEndDocument() {
        BsonContextType contextType = getContext().getContextType();

        setContext(getContext().getParentContext());
        bsonCallback.objectDone();

        if (contextType == BsonContextType.SCOPE_DOCUMENT) {
            Object scope = bsonCallback.get();
            bsonCallback = getContext().callback;
            bsonCallback.gotCodeWScope(getContext().name, getContext().code, scope);
        }
    }

    @Override
    public void doWriteStartArray() {
        bsonCallback.arrayStart(getName());
        setContext(new Context(getContext(), BsonContextType.ARRAY));
    }

    @Override
    protected void doWriteEndArray() {
        setContext(getContext().getParentContext());
        bsonCallback.arrayDone();
    }

    @Override
    protected void doWriteBinaryData(final BsonBinary binary) {
        if (binary.getType() == BsonBinarySubType.UUID_LEGACY.getValue()) {
            bsonCallback.gotUUID(getName(),
                    readLong(binary.getData(), 0),
                    readLong(binary.getData(), 8));
        } else {
            bsonCallback.gotBinary(getName(), binary.getType(), binary.getData());
        }
    }

    @Override
    public void doWriteBoolean(final boolean value) {
        bsonCallback.gotBoolean(getName(), value);
        setState(getNextState());
    }

    @Override
    protected void doWriteDateTime(final long value) {
        bsonCallback.gotDate(getName(), value);
    }

    @Override
    protected void doWriteDBPointer(final BsonDbPointer value) {
        bsonCallback.gotDBRef(getName(), value.getNamespace(), value.getId());
    }

    @Override
    protected void doWriteDouble(final double value) {
        bsonCallback.gotDouble(getName(), value);
    }

    @Override
    protected void doWriteInt32(final int value) {
        bsonCallback.gotInt(getName(), value);
    }

    @Override
    protected void doWriteInt64(final long value) {
        bsonCallback.gotLong(getName(), value);
    }

    @Override
    protected void doWriteJavaScript(final String code) {
        bsonCallback.gotCode(getName(), code);
    }

    @Override
    protected void doWriteJavaScriptWithScope(final String code) {
        getContext().callback = bsonCallback;
        getContext().code = code;
        getContext().name = getName();
        this.bsonCallback = bsonCallback.createBSONCallback();
    }

    @Override
    protected void doWriteMaxKey() {
        bsonCallback.gotMaxKey(getName());
    }

    @Override
    protected void doWriteMinKey() {
        bsonCallback.gotMinKey(getName());
    }

    @Override
    public void doWriteNull() {
        bsonCallback.gotNull(getName());
    }

    @Override
    public void doWriteObjectId(final ObjectId objectId) {
        bsonCallback.gotObjectId(getName(), objectId);
    }

    @Override
    public void doWriteRegularExpression(final BsonRegularExpression regularExpression) {
        bsonCallback.gotRegex(getName(), regularExpression.getPattern(), regularExpression.getOptions());
    }

    @Override
    public void doWriteString(final String value) {
        bsonCallback.gotString(getName(), value);
    }

    @Override
    public void doWriteSymbol(final String value) {
        bsonCallback.gotSymbol(getName(), value);
    }

    @Override
    public void doWriteTimestamp(final BsonTimestamp value) {
        bsonCallback.gotTimestamp(getName(), value.getTime(), value.getInc());
    }

    @Override
    public void doWriteUndefined() {
        bsonCallback.gotUndefined(getName());
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
