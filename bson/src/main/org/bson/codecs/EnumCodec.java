/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.bson.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;

/**
 * A codec for classes that extends {@link Enum}
 *
 * @param <T> The enum type
 * @since 4.5
 */
public final class EnumCodec<T extends Enum<T>> implements Codec<T> {
    private final Class<T> clazz;

    public EnumCodec(final Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        return Enum.valueOf(clazz, reader.readString());
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        writer.writeString(value.name());
    }

    @Override
    public Class<T> getEncoderClass() {
        return clazz;
    }
}
