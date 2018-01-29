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

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

class LazyCodec<T> implements Codec<T> {
    private final CodecRegistry registry;
    private final Class<T> clazz;
    private volatile Codec<T> wrapped;

    LazyCodec(final CodecRegistry registry, final Class<T> clazz) {
        this.registry = registry;
        this.clazz = clazz;
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        getWrapped().encode(writer, value, encoderContext);
    }

    @Override
    public Class<T> getEncoderClass() {
        return clazz;
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        return getWrapped().decode(reader, decoderContext);
    }

    private Codec<T> getWrapped() {
        if (wrapped == null) {
            wrapped = registry.get(clazz);
        }

        return wrapped;
    }
}
