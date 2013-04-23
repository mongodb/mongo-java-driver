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

import java.lang.reflect.Field;

public class PojoCodec implements CollectibleCodec<Object> {
    private final Codecs codecs;

    public PojoCodec(final Codecs codecs) {
        this.codecs = codecs;
        codecs.setDefaultObjectCodec(this);
    }

    @Override
    public Object getId(final Object document) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final Object value) {
        System.out.println("Encoding Pojo: " + value.getClass());
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
        System.out.println("Field: " + field);
        final String fieldName = field.getName();
        try {
            field.setAccessible(true);
            System.out.println("value: " + value);
            final Object fieldValue = field.get(value);
            System.out.println("fieldValue: " + fieldValue);
            System.out.println("fieldName: " + fieldName);
            bsonWriter.writeName(fieldName);
            codecs.encode(bsonWriter, fieldValue);
            field.setAccessible(false);
        } catch (IllegalAccessException e) {
            //TODO: this is really going to bugger up the writer if it throws an exception halfway through writing
            throw new EncodingException("Could not encode field '" + fieldName + "' from " + value, e);
        }
        System.out.println("\n");
    }

}
