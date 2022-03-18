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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class MapOfCodecsProvider implements CodecProvider {
    private final Map<Class<?>, Codec<?>> codecsMap = new HashMap<Class<?>, Codec<?>>();

    MapOfCodecsProvider(final List<? extends Codec<?>> codecsList) {
       for (Codec<?> codec : codecsList) {
           codecsMap.put(codec.getEncoderClass(), codec);
       }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return (Codec<T>) codecsMap.get(clazz);
    }

    @Override
    public String toString() {
        return "MapOfCodecsProvider{"
                + "codecsMap=" + codecsMap
                + '}';
    }
}
