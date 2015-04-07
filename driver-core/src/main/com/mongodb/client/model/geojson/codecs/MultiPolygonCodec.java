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

import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.model.geojson.codecs.GeometryCodecHelper.encodeGeometry;
import static com.mongodb.client.model.geojson.codecs.GeometryCodecHelper.encodePolygonCoordinates;

/**
 * A Codec for a GeoJSON MultiPolygon.
 *
 * @since 3.1
 */
public class MultiPolygonCodec implements Codec<MultiPolygon> {
    private final CodecRegistry registry;

    /**
     * Constructs an instance.
     *
     * @param registry the registry
     */
    public MultiPolygonCodec(final CodecRegistry registry) {
        this.registry = notNull("registry", registry);
    }

    @Override
    public void encode(final BsonWriter writer, final MultiPolygon value, final EncoderContext encoderContext) {
        encodeGeometry(writer, value, encoderContext, registry, new Runnable() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public void run() {
                writer.writeStartArray();
                for (PolygonCoordinates polygonCoordinates : value.getCoordinates()) {
                    encodePolygonCoordinates(writer, polygonCoordinates);
                }
                writer.writeEndArray();
            }
        });
    }

    @Override
    public Class<MultiPolygon> getEncoderClass() {
        return MultiPolygon.class;
    }

    @Override
    public MultiPolygon decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
