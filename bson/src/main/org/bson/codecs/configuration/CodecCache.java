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

package org.bson.codecs.configuration;

import org.bson.codecs.Codec;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

final class CodecCache {
    private final ConcurrentMap<Class<?>, Optional<? extends Codec<?>>> codecCache =
            new ConcurrentHashMap<Class<?>, Optional<? extends Codec<?>>>();

    public boolean containsKey(final Class<?> clazz) {
        return codecCache.containsKey(clazz);
    }

    public void put(final Class<?> clazz, final Codec<?> codec){
        codecCache.put(clazz, Optional.of(codec));
    }

    @SuppressWarnings("unchecked")
    public <T> Codec<T> getOrThrow(final Class<T> clazz) {
        if (codecCache.containsKey(clazz)) {
            Optional<? extends Codec<?>> optionalCodec = codecCache.get(clazz);
            if (!optionalCodec.isEmpty()) {
                return (Codec<T>) optionalCodec.get();
            }
        }
        throw new CodecConfigurationException(format("Can't find a codec for %s.", clazz));
    }
}
