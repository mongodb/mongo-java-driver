/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model.geojson.codecs;

import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.Position;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.model.geojson.codecs.GeometryCodecHelper.encodeGeometry;
import static com.mongodb.client.model.geojson.codecs.GeometryCodecHelper.encodePosition;

/**
 * A Codec for a GeoJSON MultiPoint.
 *
 * @since 3.1
 */
public class MultiPointCodec implements Codec<MultiPoint> {
    private final CodecRegistry registry;

    /**
     * Constructs an instance.
     *
     * @param registry the registry
     */
    public MultiPointCodec(final CodecRegistry registry) {
        this.registry = notNull("registry", registry);
    }

    @Override
    public void encode(final BsonWriter writer, final MultiPoint value, final EncoderContext encoderContext) {
        encodeGeometry(writer, value, encoderContext, registry, new Runnable() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public void run() {
                writer.writeStartArray();
                for (Position position : value.getCoordinates()) {
                    encodePosition(writer, position);
                }
                writer.writeEndArray();
            }
        });
    }

    @Override
    public Class<MultiPoint> getEncoderClass() {
        return MultiPoint.class;
    }

    @Override
    public MultiPoint decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
