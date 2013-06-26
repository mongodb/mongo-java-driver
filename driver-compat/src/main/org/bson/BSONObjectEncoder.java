/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.bson.types.CodeWScope;
import org.bson.types.Symbol;
import org.mongodb.Encoder;
import org.mongodb.MongoException;
import org.mongodb.codecs.PrimitiveCodecs;

import java.lang.reflect.Array;
import java.util.Map;

class BSONObjectEncoder implements Encoder<BSONObject> {

    private final PrimitiveCodecs primitiveCodecs;

    public BSONObjectEncoder(final PrimitiveCodecs primitiveCodecs) {
        this.primitiveCodecs = primitiveCodecs;
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final BSONObject document) {
        bsonWriter.writeStartDocument();

        for (final String key : document.keySet()) {
            bsonWriter.writeName(key);
            writeValue(bsonWriter, document.get(key));
        }
        bsonWriter.writeEndDocument();
    }

    @Override
    public Class<BSONObject> getEncoderClass() {
        return BSONObject.class;
    }

    @SuppressWarnings("unchecked")
    private void writeValue(final BSONWriter bsonWriter, final Object initialValue) {
        final Object value = BSON.applyEncodingHooks(initialValue);
        if (value instanceof BasicBSONList) {
            encodeIterable(bsonWriter, (BasicBSONList) value);
        } else if (value instanceof BSONObject) {
            encodeEmbeddedObject(bsonWriter, ((BSONObject) value).toMap());
        } else if (value instanceof Map) {
            encodeEmbeddedObject(bsonWriter, (Map<String, Object>) value);
        } else if (value instanceof Iterable) {
            encodeIterable(bsonWriter, (Iterable) value);
        } else if (value instanceof byte[]) {
            try {
                //TODO - test and tidy
                primitiveCodecs.encode(bsonWriter, new Binary((byte[]) value));
            } catch (MongoException e) {
                throw new IllegalArgumentException("Can't serialize " + value.getClass(), e);
            }
        } else if (value != null && value.getClass().isArray()) {
            encodeArray(bsonWriter, value);
        } else if (value instanceof Symbol) {
            bsonWriter.writeSymbol(((Symbol) initialValue).getSymbol());
        } else if (value instanceof CodeWScope) {
            encodeCodeWScope(bsonWriter, (CodeWScope) value);
        } else {
            try {
                primitiveCodecs.encode(bsonWriter, value);
            } catch (MongoException ex) {
                throw new IllegalArgumentException("Can't serialize " + value.getClass(), ex);
            }
        }
    }

    private void encodeEmbeddedObject(final BSONWriter bsonWriter, final Map<String, Object> document) {
        bsonWriter.writeStartDocument();

        for (final Map.Entry<String, Object> entry : document.entrySet()) {
            bsonWriter.writeName(entry.getKey());
            writeValue(bsonWriter, entry.getValue());
        }
        bsonWriter.writeEndDocument();
    }

    private void encodeArray(final BSONWriter bsonWriter, final Object value) {
        bsonWriter.writeStartArray();

        final int size = Array.getLength(value);
        for (int i = 0; i < size; i++) {
            writeValue(bsonWriter, Array.get(value, i));
        }

        bsonWriter.writeEndArray();
    }

    private void encodeIterable(final BSONWriter bsonWriter, final Iterable<?> iterable) {
        bsonWriter.writeStartArray();
        for (final Object cur : iterable) {
            writeValue(bsonWriter, cur);
        }
        bsonWriter.writeEndArray();
    }

    @SuppressWarnings("unchecked")
    private void encodeCodeWScope(final BSONWriter bsonWriter, final CodeWScope codeWScope) {
        bsonWriter.writeJavaScriptWithScope(codeWScope.getCode());
        encodeEmbeddedObject(bsonWriter, codeWScope.getScope().toMap());
    }
}
