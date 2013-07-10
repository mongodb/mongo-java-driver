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
import org.bson.BSONType;
import org.mongodb.Decoder;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PojoDecoder implements Decoder<Object> {
    private final Codecs codecs;

    //at this time, this seems to be the only way to
    @SuppressWarnings("rawtypes")
    private final Map<Class<?>, ClassModel> fieldsForClass = new HashMap<Class<?>, ClassModel>();

    public PojoDecoder(final Codecs codecs) {
        this.codecs = codecs;
    }

    @Override
    public Object decode(final BSONReader reader) {
        return decode(reader, Object.class);
    }

    public <T> T decode(final BSONReader reader, final Class<T> theClass) {
        final T pojo;
        try {
            reader.readStartDocument();
            pojo = decodePojo(reader, theClass);
            reader.readEndDocument();
        } catch (IllegalAccessException e) {
            throw new DecodingException("Could not decode into '" + theClass, e);
        }
        return pojo;
    }

    @SuppressWarnings("unchecked") // bah
    private <T> T decodePojo(final BSONReader reader, final Class<T> theClass) throws IllegalAccessException {
        ClassModel<T> classModel = fieldsForClass.get(theClass);
        if (classModel == null) {
            classModel = new ClassModel<T>(theClass);
            fieldsForClass.put(theClass, classModel);
        }
        final T pojo = classModel.createInstanceOfClass();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            final Field fieldOnPojo = getPojoFieldForNextValue(reader.readName(), classModel);
            final Object decodedValue;
            if (reader.getCurrentBSONType() == BSONType.DOCUMENT && !codecs.canDecode(fieldOnPojo.getType())) {
                decodedValue = decode(reader, fieldOnPojo.getType());
            } else if (reader.getCurrentBSONType() == BSONType.DOCUMENT) {
                decodedValue = decodeMap(reader, fieldOnPojo);
            } else if (reader.getCurrentBSONType() == BSONType.ARRAY) {
                decodedValue = decodeIterable(reader, fieldOnPojo);
            } else {
                decodedValue = codecs.decode(reader);
            }
            fieldOnPojo.setAccessible(true);
            fieldOnPojo.set(pojo, decodedValue);
            fieldOnPojo.setAccessible(false);
        }
        return pojo;
    }

    private <E> Map<String, E> decodeMap(final BSONReader reader, final Field fieldOnPojo) {
        final Type[] actualTypeArguments = ((ParameterizedType) fieldOnPojo.getGenericType()).getActualTypeArguments();
        final Type typeOfItemsInMap = actualTypeArguments[1];
        final Map<String, E> map = new HashMap<String, E>();

        reader.readStartDocument();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            final String fieldName = reader.readName();
            final E value = getValue(reader, (Class) typeOfItemsInMap);
            map.put(fieldName, value);
        }

        reader.readEndDocument();
        return map;
    }

    private <E> Iterable<E> decodeIterable(final BSONReader reader, final Field fieldOnPojo) {
        final Class<?> classOfIterable = fieldOnPojo.getType();
        if (classOfIterable.isArray()) {
            throw new DecodingException("Decoding into arrays is not supported.  Either provide a custom decoder, or use a List.");
        }

        final Collection<E> collection;
        if (Set.class.isAssignableFrom(classOfIterable)) {
            collection = new HashSet<E>();
        } else {
            collection = new ArrayList<E>();
        }

        final Type[] actualTypeArguments = ((ParameterizedType) fieldOnPojo.getGenericType()).getActualTypeArguments();
        final Type typeOfItemsInList = actualTypeArguments[0];

        reader.readStartArray();
        //TODO: this is still related to the problem that we can't reuse the IterableCodec, or tell it to call back to here
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            final E value = getValue(reader, (Class) typeOfItemsInList);
            collection.add(value);
        }

        reader.readEndArray();
        return collection;
    }

    // Need to cast into type E when decoding into a collection
    @SuppressWarnings("unchecked")
    private <E> E getValue(final BSONReader reader, final Class<?> typeOfItemsInList) {
        final E value;
        if (codecs.canDecode(typeOfItemsInList)) {
            value = (E) codecs.decode(reader);
        } else {
            value = (E) decode(reader, typeOfItemsInList);
        }
        return value;
    }

    private <T> Field getPojoFieldForNextValue(final String nameOfField, final ClassModel<T> classModel) {
        try {
            return classModel.getDeclaredField(nameOfField);
        } catch (NoSuchFieldException e) {
            throw new DecodingException(String.format("Could not decode field '%s' into '%s'", nameOfField, classModel), e);
        }
    }
}
