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

package org.bson.internal;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.internal.CodecCache.CodecCacheKey;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class ProvidersCodecRegistry implements CycleDetectingCodecRegistry {
    private final List<CodecProvider> codecProviders;
    private final CodecCache codecCache = new CodecCache();

    public ProvidersCodecRegistry(final List<? extends CodecProvider> codecProviders) {
        isTrueArgument("codecProviders must not be null or empty", codecProviders != null && codecProviders.size() > 0);
        this.codecProviders = new ArrayList<>(codecProviders);
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz) {
        return get(new ChildCodecRegistry<>(this, clazz, null));
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final List<Type> typeArguments) {
        notNull("typeArguments", typeArguments);
        isTrueArgument(format("typeArguments size should equal the number of type parameters in class %s, but is %d",
                        clazz, typeArguments.size()),
                clazz.getTypeParameters().length == typeArguments.size());
        return get(new ChildCodecRegistry<>(this, clazz, typeArguments));
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return get(clazz, Collections.emptyList(), registry);
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final List<Type> typeArguments, final CodecRegistry registry) {
        for (CodecProvider provider : codecProviders) {
            Codec<T> codec = provider.get(clazz, typeArguments, registry);
            if (codec != null) {
                return codec;
            }
        }
        return null;
    }

    public <T> Codec<T> get(final ChildCodecRegistry<T> context) {
        CodecCacheKey codecCacheKey = new CodecCacheKey(context.getCodecClass(), context.getTypes().orElse(null));
        return codecCache.<T>get(codecCacheKey).orElseGet(() -> {
            for (CodecProvider provider : codecProviders) {
                Codec<T> codec = provider.get(context.getCodecClass(), context.getTypes().orElse(emptyList()), context);
                if (codec != null) {
                    return codecCache.putIfAbsent(codecCacheKey, codec);
                }
            }
            throw new CodecConfigurationException(format("Can't find a codec for %s.", codecCacheKey));
        });
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProvidersCodecRegistry that = (ProvidersCodecRegistry) o;
        if (codecProviders.size() != that.codecProviders.size()) {
            return false;
        }
        for (int i = 0; i < codecProviders.size(); i++) {
            if (codecProviders.get(i).getClass() != that.codecProviders.get(i).getClass()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return codecProviders.hashCode();
    }

    @Override
    public String toString() {
        return "ProvidersCodecRegistry{"
                + "codecProviders=" + codecProviders
                + '}';
    }
}
