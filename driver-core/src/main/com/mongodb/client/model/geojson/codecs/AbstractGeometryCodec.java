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

package com.mongodb.client.model.geojson.codecs;

import com.mongodb.client.model.geojson.Geometry;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import static com.mongodb.client.model.geojson.codecs.GeometryDecoderHelper.decodeGeometry;
import static com.mongodb.client.model.geojson.codecs.GeometryEncoderHelper.encodeGeometry;

abstract class AbstractGeometryCodec<T extends Geometry> implements Codec<T> {
    private final CodecRegistry registry;
    private final Class<T> encoderClass;

    AbstractGeometryCodec(final CodecRegistry registry, final Class<T> encoderClass) {
        this.registry = registry;
        this.encoderClass = encoderClass;
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        encodeGeometry(writer, value, encoderContext, registry);
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        return decodeGeometry(reader, getEncoderClass());
    }

    @Override
    public Class<T> getEncoderClass() {
        return encoderClass;
    }
}
