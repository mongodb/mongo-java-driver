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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final List<CodecProvider> codecProviders;
    private final Map<Class<?>, Codec<?>> codecMap;

    /**
     * Construct a new {@code CodecRegistry} from the given list if {@code CodecProvider} instances.  The registry will use the codec
     * providers to find Codec instances, consulting each provider in order, and return the first Codec found.  Therefore,
     * care should be taken to order the codec providers to achieve the desired behavior.
     *
     * @param codecProviders the list of codec providers
     */
    public RootCodecRegistry(final List<? extends CodecProvider> codecProviders) {
        this(new ArrayList<CodecProvider>(codecProviders), new HashMap<Class<?>, Codec<?>>());
    }

    private RootCodecRegistry(final List<CodecProvider> codecProviders, final Map<Class<?>, Codec<?>> codecMap) {
        this.codecProviders = codecProviders;
        this.codecMap = codecMap;
    }

    /**
     * Creates a new registry which is identical to this with the addition of the given codec.  Any codecs added with this method will
     * take precedence over codecs that may be returned from the codec providers.
     *
     * @param codec the codec
     * @param <T> the class type of the codec
     * @return the new registry
     */
    public <T> RootCodecRegistry withCodec(final Codec<T> codec) {
        Map<Class<?>, Codec<?>> newCodecMap = new HashMap<Class<?>, Codec<?>>(codecMap);
        newCodecMap.put(codec.getEncoderClass(), codec);
        return new RootCodecRegistry(codecProviders, newCodecMap);
    }

    /**
     * Register a new codec.
     * @param codec the codec
     */
    public void register(final Codec codec) {
        codecMap.put(codec.getEncoderClass(), codec);
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
        return get(new ChildCodecRegistry<T>(this, clazz));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    <T> Codec<T> get(final ChildCodecRegistry context) {
        if (!codecs.containsKey(context.getCodecClass())) {
            Codec<T> codec = (Codec<T>) codecMap.get(context.getCodecClass());
            if (codec == null) {
                codec = getCodecFromProviders(context);
            }
            if (codec == null){
                throw new CodecConfigurationException(format("Can't find a codec for %s.", context.getCodecClass()));
            }
            codecs.putIfAbsent(context.getCodecClass(), codec);
        }

        return (Codec<T>) codecs.get(context.getCodecClass());
    }

    private <T> Codec<T> getCodecFromProviders(final ChildCodecRegistry<T> context) {
        for (CodecProvider provider : codecProviders) {
            Codec<T> result = provider.get(context.getCodecClass(), context);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RootCodecRegistry that = (RootCodecRegistry) o;

        if (!codecProviders.equals(that.codecProviders)) {
            return false;
        }

        if (!codecMap.equals(that.codecMap)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = codecProviders.hashCode();
        result = 31 * result + codecMap.hashCode();
        return result;
    }
}
