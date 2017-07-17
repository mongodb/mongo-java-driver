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

import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings({"unchecked", "rawtypes"})
final class MapCodec<T> implements Codec<Map<String, T>> {
    private final Class<Map<String, T>> encoderClass;
    private final Codec<T> codec;

    MapCodec(final Class<Map<String, T>> encoderClass, final Codec<T> codec) {
        this.encoderClass = encoderClass;
        this.codec = codec;
    }

    @Override
    public void encode(final BsonWriter writer, final Map<String, T> map, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        for (final Entry<String, T> entry : map.entrySet()) {
            writer.writeName(entry.getKey());
            codec.encode(writer, entry.getValue(), encoderContext);
        }
        writer.writeEndDocument();
    }

    @Override
    public Map<String, T> decode(final BsonReader reader, final DecoderContext context) {
        reader.readStartDocument();
        Map<String, T> map = getInstance();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            map.put(reader.readName(), codec.decode(reader, context));
        }
        reader.readEndDocument();
        return map;
    }

    @Override
    public Class<Map<String, T>> getEncoderClass() {
        return encoderClass;
    }

    private Map<String, T> getInstance() {
        try {
            return encoderClass.getDeclaredConstructor().newInstance();
        } catch (final Exception e) {
            throw new CodecConfigurationException(e.getMessage(), e);
        }
    }

}
