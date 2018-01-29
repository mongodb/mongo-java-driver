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

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import static java.lang.String.format;

final class CollectionPropertyCodecProvider implements PropertyCodecProvider {
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public <T> Codec<T> get(final TypeWithTypeParameters<T> type, final PropertyCodecRegistry registry) {
        if (Collection.class.isAssignableFrom(type.getType()) && type.getTypeParameters().size() == 1) {
            return new CollectionCodec(type.getType(), registry.get(type.getTypeParameters().get(0)));
        } else {
            return null;
        }
    }

    private static class CollectionCodec<T> implements Codec<Collection<T>> {
        private final Class<Collection<T>> encoderClass;
        private final Codec<T> codec;

        CollectionCodec(final Class<Collection<T>> encoderClass, final Codec<T> codec) {
            this.encoderClass = encoderClass;
            this.codec = codec;
        }

        @Override
        public void encode(final BsonWriter writer, final Collection<T> collection, final EncoderContext encoderContext) {
            writer.writeStartArray();
            for (final T value : collection) {
                if (value == null) {
                    writer.writeNull();
                } else {
                    codec.encode(writer, value, encoderContext);
                }
            }
            writer.writeEndArray();
        }

        @Override
        public Collection<T> decode(final BsonReader reader, final DecoderContext context) {
            Collection<T> collection = getInstance();
            reader.readStartArray();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (reader.getCurrentBsonType() == BsonType.NULL) {
                    collection.add(null);
                    reader.readNull();
                } else {
                    collection.add(codec.decode(reader, context));
                }
            }
            reader.readEndArray();
            return collection;
        }

        @Override
        public Class<Collection<T>> getEncoderClass() {
            return encoderClass;
        }

        private Collection<T> getInstance() {
            if (encoderClass.isInterface()) {
                if (encoderClass.isAssignableFrom(ArrayList.class)) {
                    return new ArrayList<T>();
                } else if (encoderClass.isAssignableFrom(HashSet.class)) {
                    return new HashSet<T>();
                } else {
                    throw new CodecConfigurationException(format("Unsupported Collection interface of %s!", encoderClass.getName()));
                }
            }

            try {
                return encoderClass.newInstance();
            } catch (final Exception e) {
                throw new CodecConfigurationException(e.getMessage(), e);
            }
        }
    }
}
