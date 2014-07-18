/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({ "unchecked", "rawtypes"})
public class ListCodec implements Codec<List> {
    private final CodecRegistry registry;
    private final BsonTypeClassMap bsonTypeClassMap;

    public ListCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap) {
        this.registry = registry;
        this.bsonTypeClassMap = bsonTypeClassMap;
    }

    @Override
    public List decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartArray();
        List list = new ArrayList();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            Object value;
            if (reader.getCurrentBsonType() == BsonType.NULL) {
                reader.readNull();
                value = null;
            } else {
                value = registry.get(bsonTypeClassMap.get(reader.getCurrentBsonType())).decode(reader, decoderContext);
            }
            list.add(value);
        }
        reader.readEndArray();
        return list;
    }

    @Override
    public void encode(final BsonWriter writer, final List list, final EncoderContext encoderContext) {
        writer.writeStartArray();
        for (final Object value : list) {
            if (value == null) {
                writer.writeNull();
            } else {
                Codec codec = registry.get(value.getClass());
                encoderContext.encodeWithChildContext(codec, writer, value);
            }
        }
        writer.writeEndArray();
    }

    @Override
    public Class<List> getEncoderClass() {
        return List.class;
    }
}
