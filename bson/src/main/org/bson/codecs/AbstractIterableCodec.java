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

import java.util.ArrayList;
import java.util.List;

abstract class AbstractIterableCodec<T> implements Codec<Iterable<T>> {

    abstract T readValue(BsonReader reader, DecoderContext decoderContext);

    abstract void writeValue(BsonWriter writer, T cur, EncoderContext encoderContext);

    @Override
    public Iterable<T> decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartArray();

        List<T> list = new ArrayList<>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            if (reader.getCurrentBsonType() == BsonType.NULL) {
                reader.readNull();
                list.add(null);
            } else {
                list.add(readValue(reader, decoderContext));
            }
        }

        reader.readEndArray();

        return list;
    }

    @Override
    public void encode(final BsonWriter writer, final Iterable<T> value, final EncoderContext encoderContext) {
        writer.writeStartArray();
        for (final T cur : value) {
            if (cur == null) {
                writer.writeNull();
            } else {
                writeValue(writer, cur, encoderContext);
            }
        }
        writer.writeEndArray();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Class<Iterable<T>> getEncoderClass() {
        return (Class<Iterable<T>>) ((Class) Iterable.class);
    }
}
