/*
 * Copyright 2014-2016 MongoDB, Inc.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A {@code BsonReader} implementation that reads from an instance of {@code BsonDocument}.  This can be used to decode a {@code
 * BsonDocument} using a {@code Decoder}.
 *
 * @see BsonDocument
 * @see org.bson.codecs.Decoder
 *
 * @since 3.0
 */
public class BsonDocumentReader extends AbstractBsonReader {
    private BsonValue currentValue;
    private Mark mark;

    /**
     * Construct a new instance.
     *
     * @param document the document to read from
     */
    public BsonDocumentReader(final BsonDocument document) {
        super();
        setContext(new Context(null, BsonContextType.TOP_LEVEL, document));
        currentValue = document;
    }

    @Override
    protected BsonBinary doReadBinaryData() {
        return currentValue.asBinary();
    }

    @Override
    protected byte doPeekBinarySubType() {
        return currentValue.asBinary().getType();
    }

    @Override
    protected boolean doReadBoolean() {
        return currentValue.asBoolean().getValue();
    }

    @Override
    protected long doReadDateTime() {
        return currentValue.asDateTime().getValue();
    }

    @Override
    protected double doReadDouble() {
        return currentValue.asDouble().getValue();
    }

    @Override
    protected void doReadEndArray() {
        setContext(getContext().getParentContext());
    }

    @Override
    protected void doReadEndDocument() {
        setContext(getContext().getParentContext());
        switch (getContext().getContextType()) {
            case ARRAY:
            case DOCUMENT:
                setState(State.TYPE);
                break;
            case TOP_LEVEL:
                setState(State.DONE);
                break;
            default:
                throw new BSONException("Unexpected ContextType.");
        }
    }

    @Override
    protected int doReadInt32() {
        return currentValue.asInt32().getValue();
    }

    @Override
    protected long doReadInt64() {
        return currentValue.asInt64().getValue();
    }

    @Override
    public Decimal128 doReadDecimal128() {
        return currentValue.asDecimal128().getValue();
    }

    @Override
    protected String doReadJavaScript() {
        return currentValue.asJavaScript().getCode();
    }

    @Override
    protected String doReadJavaScriptWithScope() {
        return currentValue.asJavaScriptWithScope().getCode();
    }

    @Override
    protected void doReadMaxKey() {
    }

    @Override
    protected void doReadMinKey() {
    }

    @Override
    protected void doReadNull() {
    }

    @Override
    protected ObjectId doReadObjectId() {
        return currentValue.asObjectId().getValue();
    }

    @Override
    protected BsonRegularExpression doReadRegularExpression() {
        return currentValue.asRegularExpression();
    }

    @Override
    protected BsonDbPointer doReadDBPointer() {
        return currentValue.asDBPointer();
    }

    @Override
    protected void doReadStartArray() {
        BsonArray array = currentValue.asArray();
        setContext(new Context(getContext(), BsonContextType.ARRAY, array));
    }

    @Override
    protected void doReadStartDocument() {
        BsonDocument document;
        if (currentValue.getBsonType() == BsonType.JAVASCRIPT_WITH_SCOPE) {
            document = currentValue.asJavaScriptWithScope().getScope();
        } else {
            document = currentValue.asDocument();
        }
        setContext(new Context(getContext(), BsonContextType.DOCUMENT, document));
    }

    @Override
    protected String doReadString() {
        return currentValue.asString().getValue();
    }

    @Override
    protected String doReadSymbol() {
        return currentValue.asSymbol().getSymbol();
    }

    @Override
    protected BsonTimestamp doReadTimestamp() {
        return currentValue.asTimestamp();
    }

    @Override
    protected void doReadUndefined() {
    }

    @Override
    protected void doSkipName() {
    }

    @Override
    protected void doSkipValue() {
    }

    @Override
    public BsonType readBsonType() {
        if (getState() == State.INITIAL || getState() == State.SCOPE_DOCUMENT) {
            // there is an implied type of Document for the top level and for scope documents
            setCurrentBsonType(BsonType.DOCUMENT);
            setState(State.VALUE);
            return getCurrentBsonType();
        }

        if (getState() != State.TYPE) {
            throwInvalidState("ReadBSONType", State.TYPE);
        }

        switch (getContext().getContextType()) {
            case ARRAY:
                currentValue = getContext().getNextValue();
                if (currentValue == null) {
                    setState(State.END_OF_ARRAY);
                    return BsonType.END_OF_DOCUMENT;
                }
                setState(State.VALUE);
                break;
            case DOCUMENT:
                Map.Entry<String, BsonValue> currentElement = getContext().getNextElement();
                if (currentElement == null) {
                    setState(State.END_OF_DOCUMENT);
                    return BsonType.END_OF_DOCUMENT;
                }
                setCurrentName(currentElement.getKey());
                currentValue = currentElement.getValue();
                setState(State.NAME);
                break;
            default:
                throw new BSONException("Invalid ContextType.");
        }

        setCurrentBsonType(currentValue.getBsonType());
        return getCurrentBsonType();
    }

    @Override
    public void mark() {
        if (mark != null) {
            throw new BSONException("A mark already exists; it needs to be reset before creating a new one");
        }
        mark = new Mark();
    }

    @Override
    public void reset() {
        if (mark == null) {
            throw new BSONException("trying to reset a mark before creating it");
        }
        mark.reset();
        mark = null;
    }

    @Override
    protected Context getContext() {
        return (Context) super.getContext();
    }
    protected class Mark extends AbstractBsonReader.Mark {
        private BsonValue currentValue;
        private Context context;

        protected Mark() {
            super();
            currentValue = BsonDocumentReader.this.currentValue;
            context = BsonDocumentReader.this.getContext();
            context.mark();
        }

        protected void reset() {
            super.reset();
            BsonDocumentReader.this.currentValue = currentValue;
            BsonDocumentReader.this.setContext(context);
            context.reset();
        }
    }

    private static class BsonDocumentMarkableIterator<T> implements Iterator<T> {

        private Iterator<T> baseIterator;
        private List<T> markIterator = new ArrayList<T>();
        private int curIndex; // index of the cursor
        private boolean marking;

        protected BsonDocumentMarkableIterator(final Iterator<T> baseIterator) {
            this.baseIterator = baseIterator;
            curIndex = 0;
            marking = false;
        }

        /**
         *
         */
        protected void mark() {
            marking = true;
        }

        /**
         *
         */
        protected void reset() {
            curIndex = 0;
            marking = false;
        }


        @Override
        public boolean hasNext() {
            return baseIterator.hasNext() || curIndex < markIterator.size();
        }

        @Override
        public T next() {
            T value;
            //TODO: check closed
            if (curIndex < markIterator.size()) {
                value = markIterator.get(curIndex);
                if (marking) {
                    curIndex++;
                } else {
                    markIterator.remove(0);
                }
            } else {
                value = baseIterator.next();
                if (marking) {
                    markIterator.add(value);
                    curIndex++;
                }
            }


            return value;
        }

        @Override
        public void remove() {
            // iterator is read only
        }
    }

    protected class Context extends AbstractBsonReader.Context {

        private BsonDocumentMarkableIterator<Map.Entry<String, BsonValue>> documentIterator;
        private BsonDocumentMarkableIterator<BsonValue> arrayIterator;

        protected Context(final Context parentContext, final BsonContextType contextType, final BsonArray array) {
            super(parentContext, contextType);
            arrayIterator = new BsonDocumentMarkableIterator<BsonValue>(array.iterator());
        }

        protected Context(final Context parentContext, final BsonContextType contextType, final BsonDocument document) {
            super(parentContext, contextType);
            documentIterator = new BsonDocumentMarkableIterator<Map.Entry<String, BsonValue>>(document.entrySet().iterator());
        }

        public Map.Entry<String, BsonValue> getNextElement() {
            if (documentIterator.hasNext()) {
                return documentIterator.next();
            } else {
                return null;
            }
        }
        protected void mark() {
            if (documentIterator != null) {
                documentIterator.mark();
            } else {
                arrayIterator.mark();
            }

            if (getParentContext() != null) {
                ((Context) getParentContext()).mark();
            }
        }

        protected void reset() {
            if (documentIterator != null) {
                documentIterator.reset();
            } else {
                arrayIterator.reset();
            }

            if (getParentContext() != null) {
                ((Context) getParentContext()).reset();
            }
        }

        public BsonValue getNextValue() {
            if (arrayIterator.hasNext()) {
                return arrayIterator.next();
            } else {
                return null;
            }
        }
    }
}
