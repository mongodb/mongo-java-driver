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

public class PojoDecoder implements Decoder {
    private final Codecs codecs;

    public PojoDecoder(final Codecs codecs) {
        this.codecs = codecs;
    }

    @Override
    public Object decode(final BSONReader reader) {
        return decode(reader, Object.class);
    }

    public <T> T decode(final BSONReader reader, final Class<T> theClass) {
        T pojo;
        try {
            reader.readStartDocument();
            pojo = decodePojo(reader, theClass);
            reader.readEndDocument();
        } catch (IllegalAccessException e) {
            throw new DecodingException("Could not decode into '" + theClass, e);
        }
        return pojo;
    }

    private <T> T decodePojo(final BSONReader reader, final Class<T> theClass) throws IllegalAccessException {
        final T pojo = createInstanceOfClass(theClass);
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            final Field fieldOnPojo = getPojoFieldForNextValue(reader.readName(), theClass);
            Object decodedValue;
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

    private Map decodeMap(final BSONReader reader, final Field fieldOnPojo) {
        final Type[] actualTypeArguments = ((ParameterizedType) fieldOnPojo.getGenericType()).getActualTypeArguments();
        final Type typeOfItemsInMap = actualTypeArguments[1];
        final Map map = new HashMap();

        reader.readStartDocument();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            final String fieldName = reader.readName();
            final Object value = decode(reader, (Class) typeOfItemsInMap);
            map.put(fieldName, value);
        }

        reader.readEndDocument();
        return map;
    }

    private Iterable decodeIterable(final BSONReader reader, final Field fieldOnPojo) {
        if (Set.class.isAssignableFrom(fieldOnPojo.getType())) {
            return decodeCollection(reader, fieldOnPojo, new HashSet());
        } else {
            return decodeCollection(reader, fieldOnPojo, new ArrayList());
        }
    }

    private Collection decodeCollection(final BSONReader reader, final Field fieldOnPojo, final Collection collection) {
        final Type[] actualTypeArguments = ((ParameterizedType) fieldOnPojo.getGenericType()).getActualTypeArguments();
        final Type typeOfItemsInList = actualTypeArguments[0];

        reader.readStartArray();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            final Object value = decode(reader, (Class) typeOfItemsInList);
            collection.add(value);
        }

        reader.readEndArray();
        return collection;
    }

    private <T> T createInstanceOfClass(final Class<T> classToCreate) throws IllegalAccessException {
        try {
            return classToCreate.newInstance();
        } catch (InstantiationException e) {
            throw new DecodingException(String.format("Can't create an instance of %s", classToCreate), e);
        }
    }

    private Field getPojoFieldForNextValue(final String nameOfField, final Class<?> classCurrentlyBeingDecoded) {
        try {
            return classCurrentlyBeingDecoded.getDeclaredField(nameOfField);
        } catch (NoSuchFieldException e) {
            throw new DecodingException(String.format("Could not decode field '%s' into '%s'", nameOfField, classCurrentlyBeingDecoded), e);
        }
    }
}
