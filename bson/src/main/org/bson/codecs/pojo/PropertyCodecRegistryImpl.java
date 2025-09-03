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

package org.bson.codecs.pojo;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

class PropertyCodecRegistryImpl implements PropertyCodecRegistry {
    private final List<PropertyCodecProvider> propertyCodecProviders;
    private final ConcurrentHashMap<TypeWithTypeParameters<?>, Codec<?>> propertyCodecCache;

    PropertyCodecRegistryImpl(final PojoCodec<?> pojoCodec, final CodecRegistry codecRegistry,
                              final List<PropertyCodecProvider> propertyCodecProviders) {
        List<PropertyCodecProvider> augmentedProviders = new ArrayList<>();
        if (propertyCodecProviders != null) {
            augmentedProviders.addAll(propertyCodecProviders);
        }
        augmentedProviders.add(new CollectionPropertyCodecProvider());
        augmentedProviders.add(new MapPropertyCodecProvider());
        augmentedProviders.add(new EnumPropertyCodecProvider(codecRegistry));
        augmentedProviders.add(new FallbackPropertyCodecProvider(pojoCodec, codecRegistry));
        this.propertyCodecProviders = augmentedProviders;
        this.propertyCodecCache = new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S> Codec<S> get(final TypeWithTypeParameters<S> typeWithTypeParameters) {
        if (propertyCodecCache.containsKey(typeWithTypeParameters)) {
            return (Codec<S>) propertyCodecCache.get(typeWithTypeParameters);
        }

        for (PropertyCodecProvider propertyCodecProvider : propertyCodecProviders) {
                Codec<S> codec = propertyCodecProvider.get(typeWithTypeParameters, this);
                if (codec != null) {
                    propertyCodecCache.put(typeWithTypeParameters, codec);
                    return codec;
                }
        }
        return null;
    }
}
