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

package org.bson.codecs.record;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.lang.reflect.Type;
import java.util.List;

import static org.bson.assertions.Assertions.assertNotNull;

/**
 * Provides Codec instances for Java records.
 *
 * @since 4.6
 * @see Record
 */
public final class RecordCodecProvider implements CodecProvider {
    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return get(clazz, List.of(), registry);
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final List<Type> typeArguments, final CodecRegistry registry) {
        if (!assertNotNull(clazz).isRecord()) {
            return null;
        }
        @SuppressWarnings({"unchecked", "rawtypes"})
        Codec<T> result = new RecordCodec(clazz, assertNotNull(typeArguments), registry);
        return result;
    }
}
