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

package org.mongodb.codecs;

import org.bson.BSONWriter;
import org.mongodb.Encoder;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class PojoEncoder<T> implements Encoder<T> {
    private final Codecs codecs;

    //at this time, this seems to be the only way to
    @SuppressWarnings("rawtypes")
    private final Map<Class<?>, ClassModel> classModelForClass = new HashMap<Class<?>, ClassModel>();

    public PojoEncoder(final Codecs codecs) {
        this.codecs = codecs;
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final T value) {
        bsonWriter.writeStartDocument();
        encodePojo(bsonWriter, value);
        bsonWriter.writeEndDocument();
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) //bah.  maybe this isn't even correct
    private void encodePojo(final BSONWriter bsonWriter, final T value) {
        ClassModel<T> classModel = classModelForClass.get(value.getClass());
        if (classModel == null) {
            classModel = new ClassModel(value.getClass());
            classModelForClass.put(value.getClass(), classModel);
        }
        for (final Field field : classModel.getFields()) {
            encodeField(bsonWriter, value, field, field.getName());
        }
    }

    // need to cast the field
    @SuppressWarnings("unchecked")
    private void encodeField(final BSONWriter bsonWriter, final T value, final Field field, final String fieldName) {
        try {
            field.setAccessible(true);
            T fieldValue = (T) field.get(value);
            bsonWriter.writeName(fieldName);
            encodeValue(bsonWriter, fieldValue);
            field.setAccessible(false);
        } catch (IllegalAccessException e) {
            //TODO: this is really going to bugger up the writer if it throws an exception halfway through writing
            throw new EncodingException("Could not encode field '" + fieldName + "' from " + value, e);
        }
    }

    private void encodeValue(final BSONWriter bsonWriter, final T fieldValue) {
        if (codecs.canEncode(fieldValue)) {
            codecs.encode(bsonWriter, fieldValue);
        } else {
            encode(bsonWriter, fieldValue);
        }
    }

    @Override
    public Class<T> getEncoderClass() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
