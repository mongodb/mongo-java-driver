/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson.codecs;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.configuration.CodecConfigurationException;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Supplier;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;

abstract class AbstractMapCodec<T, M extends Map<String, T>> implements Codec<M> {

    private final Supplier<M> supplier;
    private final Class<M> clazz;

    @SuppressWarnings({"unchecked", "UnnecessaryLocalVariable", "rawtypes"})
    AbstractMapCodec(@Nullable final Class<M> clazz) {
        this.clazz = notNull("clazz", clazz);
        Class rawClass = clazz;
        if (rawClass == Map.class || rawClass == AbstractMap.class || rawClass == HashMap.class) {
            supplier = () -> (M) new HashMap<String, T>();
        } else if (rawClass == NavigableMap.class || rawClass == TreeMap.class) {
            supplier = () -> (M) new TreeMap<String, T>();
        } else {
            Constructor<? extends Map<?, ?>> constructor;
            Supplier<M> supplier;
            try {
                constructor = clazz.getDeclaredConstructor();
                supplier = () -> {
                    try {
                        return (M) constructor.newInstance();
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw new CodecConfigurationException("Can not invoke no-args constructor for Map class %s", e);
                    }
                };
            } catch (NoSuchMethodException e) {
                supplier = () -> {
                    throw new CodecConfigurationException(format("Map class %s has no public no-args constructor", clazz), e);
                };
            }
            this.supplier = supplier;
        }
    }

    abstract T readValue(BsonReader reader, DecoderContext decoderContext);

    abstract void writeValue(BsonWriter writer, T value, EncoderContext encoderContext);

    @Override
    public void encode(final BsonWriter writer, final M map, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        for (final Map.Entry<String, T> entry : map.entrySet()) {
            writer.writeName(entry.getKey());
            T value = entry.getValue();
            if (value == null) {
                writer.writeNull();
            } else {
                writeValue(writer, value, encoderContext);
            }
        }
        writer.writeEndDocument();
    }


    @Override
    public M decode(final BsonReader reader, final DecoderContext decoderContext) {
        M map = supplier.get();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            if (reader.getCurrentBsonType() == BsonType.NULL) {
                reader.readNull();
                map.put(fieldName, null);
            } else {
                map.put(fieldName, readValue(reader, decoderContext));
            }
        }

        reader.readEndDocument();
        return map;
    }

    @Override
    public Class<M> getEncoderClass() {
        return clazz;
    }
}
