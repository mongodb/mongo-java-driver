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

package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.mongodb.CollectibleCodec;
import org.mongodb.MongoClientException;

import java.lang.reflect.Field;

public class PojoCodec implements CollectibleCodec<Object> {
    private final PrimitiveCodecs primitiveCodecs;

    public PojoCodec(final PrimitiveCodecs primitiveCodecs) {
        this.primitiveCodecs = primitiveCodecs;
    }

    @Override
    public Object getId(final Object document) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final Object value) {
        bsonWriter.writeStartDocument();
        encodePojo(bsonWriter, value);
        bsonWriter.writeEndDocument();
    }

    @Override
    public Object decode(final BSONReader reader) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public Class<Object> getEncoderClass() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    private void encodePojo(final BSONWriter bsonWriter, final Object value) {

        for (Field field : value.getClass().getDeclaredFields()) {
            encodeField(bsonWriter, value, field);
        }

    }

    private void encodeField(final BSONWriter bsonWriter, final Object value, final Field field) {
        final String fieldName = field.getName();
        try {
            field.setAccessible(true);
            System.out.println("value: " + value);
            final Object fieldValue = field.get(value);
            System.out.println("fieldValue:" + fieldValue);
            if (isBSONPrimitive(fieldValue)) {
                bsonWriter.writeName(fieldName);
                primitiveCodecs.encode(bsonWriter, fieldValue);
            }
            else if (fieldValue.getClass().isArray()) {
                bsonWriter.writeStartArray(fieldName);
                encodeArray(bsonWriter, fieldValue, fieldName);
                bsonWriter.writeEndArray();
            }
            else {
                bsonWriter.writeStartDocument(fieldName);
                encodePojo(bsonWriter, fieldValue);
                bsonWriter.writeEndDocument();
            }
            field.setAccessible(false);
        } catch (IllegalAccessException e) {
            //TODO: this is really going to bugger up the writer if it throws an exception halfway through writing
            throw new EncodingException("Could not encode field '" + fieldName + "' from " + value, e);
        } catch (NoSuchFieldException e) {
            //TODO: this is really going to bugger up the writer if it throws an exception halfway through writing
            throw new EncodingException("Could not encode field '" + fieldName + "' from " + value, e);
        }
    }

    private void encodeArray(final BSONWriter bsonWriter, final Object fieldValue, final String fieldName)
    throws NoSuchFieldException {
        final Object[] array = (Object[]) fieldValue;
        for (int i = 0; i < array.length; i++) {
            final Object value = array[i];
            encodeField(bsonWriter, value, null);

        }
    }

    private boolean isBSONPrimitive(final Object value) {
        return primitiveCodecs.canEncode(value.getClass());
    }

    private static class EncodingException extends MongoClientException {
        private static final long serialVersionUID = -8147079320437509154L;

        public EncodingException(final String message, final Exception e) {
            super(message, e);
        }
    }
}
