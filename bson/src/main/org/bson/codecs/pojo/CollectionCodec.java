/*
 * Copyright 2017 MongoDB, Inc.
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

import java.util.Collection;


class CollectionCodec<T> implements Codec<Collection<T>> {
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
            codec.encode(writer, value, encoderContext);
        }
        writer.writeEndArray();
    }

    @Override
    public Collection<T> decode(final BsonReader reader, final DecoderContext context) {
        Collection<T> collection = getInstance();
        reader.readStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            collection.add(codec.decode(reader, context));
        }
        reader.readEndArray();
        return collection;
    }

    @Override
    public Class<Collection<T>> getEncoderClass() {
        return encoderClass;
    }

    private Collection<T> getInstance() {
        try {
            return encoderClass.newInstance();
        } catch (final Exception e) {
            throw new CodecConfigurationException(e.getMessage(), e);
        }
    }
}
