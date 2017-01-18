/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

import org.bson.types.Decimal128;
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

    /**
     * Gets the document that the writer is writing to.
     *
     * @return the document
     */
    public BsonDocument getDocument() {
        return document;
    }

    @Override
    protected void doWriteStartDocument() {
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
    }

    @Override
    protected void doWriteEndDocument() {
        BsonValue value = getContext().container;
        setContext(getContext().getParentContext());

        if (getContext().getContextType() == BsonContextType.JAVASCRIPT_WITH_SCOPE) {
            BsonDocument scope = (BsonDocument) value;
            BsonString code = (BsonString) getContext().container;
            setContext(getContext().getParentContext());
            write(new BsonJavaScriptWithScope(code.getValue(), scope));
        } else if (getContext().getContextType() != BsonContextType.TOP_LEVEL) {
            write(value);
        }
    }

    @Override
    protected void doWriteStartArray() {
        setContext(new Context(new BsonArray(), BsonContextType.ARRAY, getContext()));
    }

    @Override
    protected void doWriteEndArray() {
        BsonValue array = getContext().container;
        setContext(getContext().getParentContext());
        write(array);
    }

    @Override
    protected void doWriteBinaryData(final BsonBinary value) {
        write(value);
    }

    @Override
    public void doWriteBoolean(final boolean value) {
        write(BsonBoolean.valueOf(value));
    }

    @Override
    protected void doWriteDateTime(final long value) {
        write(new BsonDateTime(value));
    }

    @Override
    protected void doWriteDBPointer(final BsonDbPointer value) {
        write(value);
    }

    @Override
    protected void doWriteDouble(final double value) {
        write(new BsonDouble(value));
    }

    @Override
    protected void doWriteInt32(final int value) {
        write(new BsonInt32(value));
    }

    @Override
    protected void doWriteInt64(final long value) {
        write(new BsonInt64(value));
    }

    @Override
    protected void doWriteDecimal128(final Decimal128 value) {
        write(new BsonDecimal128(value));
    }

    @Override
    protected void doWriteJavaScript(final String value) {
        write(new BsonJavaScript(value));
    }

    @Override
    protected void doWriteJavaScriptWithScope(final String value) {
        setContext(new Context(new BsonString(value), BsonContextType.JAVASCRIPT_WITH_SCOPE, getContext()));
    }

    @Override
    protected void doWriteMaxKey() {
        write(new BsonMaxKey());
    }

    @Override
    protected void doWriteMinKey() {
        write(new BsonMinKey());
    }

    @Override
    public void doWriteNull() {
        write(BsonNull.VALUE);
    }

    @Override
    public void doWriteObjectId(final ObjectId value) {
        write(new BsonObjectId(value));
    }

    @Override
    public void doWriteRegularExpression(final BsonRegularExpression value) {
        write(value);
    }

    @Override
    public void doWriteString(final String value) {
        write(new BsonString(value));
    }

    @Override
    public void doWriteSymbol(final String value) {
        write(new BsonSymbol(value));
    }

    @Override
    public void doWriteTimestamp(final BsonTimestamp value) {
        write(value);
    }

    @Override
    public void doWriteUndefined() {
        write(new BsonUndefined());
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

        Context(final BsonValue container, final BsonContextType contextType, final Context parent) {
            super(parent, contextType);
            this.container = container;
        }

        Context() {
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
