/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo.entities;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.pojo.PropertyCodecProvider;
import org.bson.codecs.pojo.PropertyCodecRegistry;
import org.bson.codecs.pojo.TypeWithTypeParameters;

import java.util.HashMap;
import java.util.Map;

public class InvalidMapPropertyCodecProvider implements PropertyCodecProvider {

    @SuppressWarnings("unchecked")
    @Override
    public <T> Codec<T> get(final TypeWithTypeParameters<T> type, final PropertyCodecRegistry registry) {
        if (Map.class.isAssignableFrom(type.getType()) && type.getTypeParameters().size() == 2
                && type.getTypeParameters().get(0).getType().equals(Integer.class)
                && type.getTypeParameters().get(1).getType().equals(Integer.class)) {
            return (Codec<T>) new InvalidMapModelCodec((Class<Map<Integer, Integer>>) type.getType());
        } else {
            return null;
        }
    }

    private static final class InvalidMapModelCodec implements Codec<Map<Integer, Integer>> {
        private Class<Map<Integer, Integer>> encoderClass;

        private InvalidMapModelCodec(final Class<Map<Integer, Integer>> encoderClass) {
            this.encoderClass = encoderClass;
        }

        @Override
        public Map<Integer, Integer> decode(final BsonReader reader, final DecoderContext decoderContext) {
            Map<Integer, Integer> map = new HashMap<Integer, Integer>();

            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                map.put(Integer.valueOf(reader.readName()), reader.readInt32());
            }
            reader.readEndDocument();
            return map;
        }

        @Override
        public void encode(final BsonWriter writer, final Map<Integer, Integer> value, final EncoderContext encoderContext) {
            writer.writeStartDocument();
            for (Map.Entry<Integer, Integer> entry : value.entrySet()) {
                writer.writeInt32(entry.getKey().toString(), entry.getValue());
            }
            writer.writeEndDocument();
        }

        @Override
        public Class<Map<Integer, Integer>> getEncoderClass() {
            return encoderClass;
        }
    }
}
