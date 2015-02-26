/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.configuration;

import org.bson.codecs.Codec;

import static org.bson.assertions.Assertions.notNull;

final class CompoundCodecRegistry implements CodecRegistry {
    private final CodecRegistry firstCodecRegistry;
    private final CodecRegistry secondCodecRegistry;
    private final CodecCache codecCache = new CodecCache();


    CompoundCodecRegistry(final CodecRegistry firstCodecRegistry, final CodecRegistry secondCodecRegistry) {
        this.firstCodecRegistry = notNull("firstCodecRegistry", firstCodecRegistry);
        this.secondCodecRegistry = notNull("secondCodecRegistry", secondCodecRegistry);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz) {
        if (!codecCache.containsKey(clazz)) {
            try {
                Codec<T> codec = firstCodecRegistry.get(clazz);
                codecCache.put(clazz, codec);
            } catch (CodecConfigurationException e) {
                try {
                    Codec<T> codec = secondCodecRegistry.get(clazz);
                    codecCache.put(clazz, codec);
                } catch (CodecConfigurationException e1) {
                    codecCache.put(clazz, null);
                }
            }
        }
        return codecCache.getOrThrow(clazz);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompoundCodecRegistry that = (CompoundCodecRegistry) o;

        if (!secondCodecRegistry.equals(that.secondCodecRegistry)) {
            return false;
        } else if (!firstCodecRegistry.equals(that.firstCodecRegistry)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = firstCodecRegistry.hashCode();
        result = 31 * result + secondCodecRegistry.hashCode();
        return result;
    }
}
