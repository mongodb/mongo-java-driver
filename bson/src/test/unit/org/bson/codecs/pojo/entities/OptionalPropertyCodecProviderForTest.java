/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bson.codecs.pojo.entities;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.pojo.PropertyCodecProvider;
import org.bson.codecs.pojo.PropertyCodecRegistry;
import org.bson.codecs.pojo.TypeWithTypeParameters;

public class OptionalPropertyCodecProviderForTest implements PropertyCodecProvider {
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> Codec<T> get(final TypeWithTypeParameters<T> type, final PropertyCodecRegistry registry) {
        if (Optional.class.isAssignableFrom(type.getType()) && type.getTypeParameters().size() == 1) {
            Codec<?> valueCodec = registry.get(type.getTypeParameters().get(0));
            return new OptionalCodec(type.getType(), valueCodec);
        } else {
            return null;
        }
    }

    private static final class OptionalCodec<T> implements Codec<Optional<T>> {
        private final Class<Optional<T>> encoderClass;
        private final Codec<T> codec;

        private OptionalCodec(final Class<Optional<T>> encoderClass, final Codec<T> codec) {
            this.encoderClass = encoderClass;
            this.codec = codec;
        }

        @Override
        public void encode(final BsonWriter writer, final Optional<T> optionalValue, final EncoderContext encoderContext) {
            if (optionalValue == null || optionalValue.isEmpty()) {
                writer.writeNull();
            } else {
                codec.encode(writer, optionalValue.get(), encoderContext);
            }
        }

        @Override
        public Optional<T> decode(final BsonReader reader, final DecoderContext context) {
            return Optional.of(codec.decode(reader, context));
        }

        @Override
        public Class<Optional<T>> getEncoderClass() {
            return encoderClass;
        }
    }
}
