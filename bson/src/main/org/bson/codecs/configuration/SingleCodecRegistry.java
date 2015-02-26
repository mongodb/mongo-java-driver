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

import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;


final class SingleCodecRegistry<T> implements CodecRegistry {
    private final Codec<T> codec;

    SingleCodecRegistry(final Codec<T> codec) {
        this.codec = notNull("codec", codec);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz) {
        if (codec.getEncoderClass() == clazz) {
            return (Codec<T>) codec;
        }
        throw new CodecConfigurationException(format("Can't find a codec for %s.", clazz));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SingleCodecRegistry that = (SingleCodecRegistry) o;
        if (!codec.equals(that.codec)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return codec.hashCode();
    }
}
