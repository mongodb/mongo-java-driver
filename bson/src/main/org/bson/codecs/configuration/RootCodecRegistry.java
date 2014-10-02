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

package org.bson.codecs.configuration;

import org.bson.codecs.Codec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

/**
 * A registry of Codec instances searchable by the class that the Codec can encode and decode. This class can handle cycles of
 * Codec dependencies, i.e when the construction of a Codec for class A requires the construction of a Codec for class B, and vice versa.
 *
 * @since 3.0
 */
public class RootCodecRegistry implements CodecRegistry {
    private final ConcurrentMap<Class<?>, Codec<?>> codecs = new ConcurrentHashMap<Class<?>, Codec<?>>();
    private final List<CodecProvider> sources;

    /**
     * Construct a new {@code CodecRegistry} from the given list if {@code CodecProvider} instances.  The registry will use the codec
     * providers to find Codec instances, consulting each provider in order, and return the first Codec found.  Therefore,
     * care should be taken to order the codec providers to achieve the desired behavior.
     *
     * @param codecProviders the list of codec providers
     */
    public RootCodecRegistry(final List<? extends CodecProvider> codecProviders) {
        this.sources = new ArrayList<CodecProvider>(codecProviders);
    }

    /**
     * Gets a {@code Codec} for the given Class.
     *
     * @param clazz the class
     * @param <T> the class type
     * @return a codec for the given class
     * @throws CodecConfigurationException if the registry does not contain a codec for the given class.
     */
    @Override
    public <T> Codec<T> get(final Class<T> clazz) {
        Codec<T> result = get(new ChildCodecRegistry<T>(this, clazz));

        if (result == null) {
            throw new CodecConfigurationException(format("Can't find a codec for %s.", clazz));
        }

        return result;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    <T> Codec<T> get(final ChildCodecRegistry context) {
        if (!codecs.containsKey(context.getCodecClass())) {
            Codec<T> codec = getCodecFromSources(context);
            if (codec == null){
                throw new CodecConfigurationException(format("Can't find a codec for %s.", context.getCodecClass()));
            }
            codecs.putIfAbsent(context.getCodecClass(), codec);
        }

        return (Codec<T>) codecs.get(context.getCodecClass());
    }

    private <T> Codec<T> getCodecFromSources(final ChildCodecRegistry<T> context) {
        for (CodecProvider source : sources) {
            Codec<T> result = source.get(context.getCodecClass(), context);
            if (result != null) {
                return result;
            }
        }

        return null;
    }
}
