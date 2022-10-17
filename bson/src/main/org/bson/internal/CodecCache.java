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

import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.bson.assertions.Assertions.assertNotNull;

final class CodecCache {

    static final class CodecCacheKey {
        private final Class<?> clazz;
        private final List<Type> types;

        CodecCacheKey(final Class<?> clazz, final List<Type> types) {
            this.clazz = clazz;
            this.types = types;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CodecCacheKey that = (CodecCacheKey) o;
            return clazz.equals(that.clazz) && Objects.equals(types, that.types);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clazz, types);
        }

        @Override
        public String toString() {
            return "CodecCacheKey{"
                    + "clazz=" + clazz
                    + ", types=" + types
                    + '}';
        }
    }

    private final ConcurrentMap<CodecCacheKey, Codec<?>> codecCache = new ConcurrentHashMap<>();

    public <T> Codec<T> putIfAbsent(final CodecCacheKey codecCacheKey, final Codec<T> codec) {
        assertNotNull(codec);
        @SuppressWarnings("unchecked")
        Codec<T> prevCodec = (Codec<T>) codecCache.putIfAbsent(codecCacheKey, codec);
        return prevCodec == null ? codec : prevCodec;
    }

    public <T> Optional<Codec<T>> get(final CodecCacheKey codecCacheKey) {
        @SuppressWarnings("unchecked")
        Codec<T> codec = (Codec<T>) codecCache.get(codecCacheKey);
        return Optional.ofNullable(codec);
    }
}
