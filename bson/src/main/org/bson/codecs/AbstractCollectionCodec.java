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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;

abstract class AbstractCollectionCodec<T, C extends Collection<T>> implements Codec<C> {

    private final Class<C> clazz;
    private final Supplier<C> supplier;

    @SuppressWarnings({"unchecked", "UnnecessaryLocalVariable", "rawtypes"})
    AbstractCollectionCodec(final Class<C> clazz) {
        this.clazz = notNull("clazz", clazz);
        Class rawClass = clazz;
        if (rawClass == Collection.class || rawClass == List.class || rawClass == AbstractCollection.class || rawClass == AbstractList.class
                || rawClass == ArrayList.class) {
            supplier = () -> (C) new ArrayList<T>();
        } else if (rawClass == Set.class || rawClass == AbstractSet.class || rawClass == HashSet.class) {
            supplier = () -> (C) new HashSet<T>();
        } else if (rawClass == NavigableSet.class || rawClass == SortedSet.class || rawClass == TreeSet.class) {
            //noinspection SortedCollectionWithNonComparableKeys
            supplier = () -> (C) new TreeSet<T>();
        } else {
            Constructor<? extends Collection<?>> constructor;
            Supplier<C> supplier;
            try {
                constructor = clazz.getDeclaredConstructor();
                supplier = () -> {
                    try {
                        return (C) constructor.newInstance();
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw new CodecConfigurationException(format("Can not invoke no-args constructor for Collection class %s", clazz),
                                e);
                    }
                };
            } catch (NoSuchMethodException e) {
                supplier = () -> {
                    throw new CodecConfigurationException(format("No no-args constructor for Collection class %s", clazz), e);
                };
            }
            this.supplier = supplier;
        }
    }

    abstract T readValue(BsonReader reader, DecoderContext decoderContext);

    abstract void writeValue(BsonWriter writer, T cur, EncoderContext encoderContext);

    @Override
    public C decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartArray();

        C collection = supplier.get();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            if (reader.getCurrentBsonType() == BsonType.NULL) {
                reader.readNull();
                collection.add(null);
            } else {
                collection.add(readValue(reader, decoderContext));
            }
        }

        reader.readEndArray();

        return collection;
    }

    @Override
    public void encode(final BsonWriter writer, final C value, final EncoderContext encoderContext) {
        writer.writeStartArray();
        for (final T cur : value) {
            if (cur == null) {
                writer.writeNull();
            } else {
                writeValue(writer, cur, encoderContext);
            }
        }
        writer.writeEndArray();
    }

    @Override
    public Class<C> getEncoderClass() {
        return clazz;
    }
}
