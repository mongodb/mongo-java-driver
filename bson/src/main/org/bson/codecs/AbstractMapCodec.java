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

package org.bson.codecs;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMapCodec<T> implements Codec<Map<String, T>> {

    abstract T readValue(BsonReader reader, DecoderContext decoderContext);

    abstract void writeValue(BsonWriter writer, T value, EncoderContext encoderContext);

    @Override
    public void encode(final BsonWriter writer, final Map<String, T> map, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        for (final Map.Entry<String, T> entry : map.entrySet()) {
            writer.writeName(entry.getKey());
            T value = entry.getValue();
            if (value == null) {
                writer.writeNull();
            } else {
                writeValue(writer, value, encoderContext);
            }
        }
        writer.writeEndDocument();
    }


    @Override
    public Map<String, T> decode(final BsonReader reader, final DecoderContext decoderContext) {
        Map<String, T> map = new HashMap<>();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            if (reader.getCurrentBsonType() == BsonType.NULL) {
                reader.readNull();
                map.put(fieldName, null);
            } else {
                map.put(fieldName, readValue(reader, decoderContext));
            }
        }

        reader.readEndDocument();
        return map;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Class<Map<String, T>> getEncoderClass() {
        return (Class<Map<String, T>>) ((Class) Map.class);
    }
}
