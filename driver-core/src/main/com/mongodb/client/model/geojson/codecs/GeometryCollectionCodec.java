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

import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.GeometryCollection;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.model.geojson.codecs.GeometryCodecHelper.encodeCoordinateReferenceSystem;
import static com.mongodb.client.model.geojson.codecs.GeometryCodecHelper.encodeType;

/**
 * A Codec for a GeoJSON GeometryCollection.
 *
 * @since 3.1
 */
public class GeometryCollectionCodec implements Codec<GeometryCollection> {
    private final CodecRegistry registry;

    /**
     * Constructs an instance.
     *
     * @param registry the registry
     */
    public GeometryCollectionCodec(final CodecRegistry registry) {
        this.registry = notNull("registry", registry);
    }

    @Override
    public void encode(final BsonWriter writer, final GeometryCollection value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        encodeType(writer, value);

        writer.writeName("geometries");

        writer.writeStartArray();
        for (Geometry geometry : value.getGeometries()) {
            encodeGeometry(writer, geometry, encoderContext);
        }
        writer.writeEndArray();

        encodeCoordinateReferenceSystem(writer, value, encoderContext, registry);
        writer.writeEndDocument();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void encodeGeometry(final BsonWriter writer, final Geometry geometry, final EncoderContext encoderContext) {
        Codec codec = registry.get(geometry.getClass());
        encoderContext.encodeWithChildContext(codec, writer, geometry);
    }

    @Override
    public Class<GeometryCollection> getEncoderClass() {
        return GeometryCollection.class;
    }

    @Override
    public GeometryCollection decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
