/*
 * Copyright 2008-2016 MongoDB, Inc.
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

package com.mongodb;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * A codec for a BasicDBList.
 *
 * @since 3.3
 */
public class BasicDBListCodec extends AbstractDBObjectCodec implements Codec<BasicDBList> {

    /**
     * Construct an instance with the given codec registry.
     *
     * @param codecRegistry the non-null codec registry
     */
    public BasicDBListCodec(final CodecRegistry codecRegistry) {
        this(codecRegistry, DEFAULT_BSON_TYPE_CLASS_MAP);
    }

    /**
     * Construct an instance.
     *
     * @param codecRegistry the codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     */
    public BasicDBListCodec(final CodecRegistry codecRegistry, final BsonTypeClassMap bsonTypeClassMap) {
        this(codecRegistry, bsonTypeClassMap, new BasicDBObjectFactory());
    }

    /**
     * Construct an instance.
     *
     * @param codecRegistry the non-null codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     * @param objectFactory the non-null object factory used to create empty DBObject instances when decoding
     */
    public BasicDBListCodec(final CodecRegistry codecRegistry, final BsonTypeClassMap bsonTypeClassMap,
            final DBObjectFactory objectFactory) {
        super(codecRegistry, bsonTypeClassMap, objectFactory);
    }

    @Override
    public void encode(final BsonWriter writer, final BasicDBList list, final EncoderContext encoderContext) {
        writer.writeStartArray();

        for (Object element : list) {
            writeValue(writer, encoderContext, element);
        }

        writer.writeEndArray();
    }

    @Override
    public BasicDBList decode(final BsonReader reader, final DecoderContext decoderContext) {
        List<String> path = new ArrayList<String>(10);
        return readArray(reader, decoderContext, path);
    }

    @Override
    public Class<BasicDBList> getEncoderClass() {
        return BasicDBList.class;
    }

}

