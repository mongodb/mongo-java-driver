/*
 * Copyright 2015 MongoDB, Inc.
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
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;

final class GeometryCodecHelper {

    static void encodeGeometry(final BsonWriter writer, final Geometry geometry, final EncoderContext encoderContext,
                               final CodecRegistry registry, final Runnable coordinatesEncoder) {
        writer.writeStartDocument();
        encodeType(writer, geometry);
        writer.writeName("coordinates");

        coordinatesEncoder.run();

        encodeCoordinateReferenceSystem(writer, geometry, encoderContext, registry);

        writer.writeEndDocument();
    }

    static void encodeType(final BsonWriter writer, final Geometry geometry) {
        writer.writeString("type", geometry.getType().getTypeName());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static void encodeCoordinateReferenceSystem(final BsonWriter writer, final Geometry geometry,
                                                        final EncoderContext encoderContext, final CodecRegistry registry) {
        if (geometry.getCoordinateReferenceSystem() != null) {
            writer.writeName("crs");
            Codec codec = registry.get(geometry.getCoordinateReferenceSystem().getClass());
            encoderContext.encodeWithChildContext(codec, writer, geometry.getCoordinateReferenceSystem());
        }
    }

    static void encodePolygonCoordinates(final BsonWriter writer, final PolygonCoordinates polygonCoordinates) {
        writer.writeStartArray();
        encodeLinearRing(polygonCoordinates.getExterior(), writer);
        for (List<Position> ring : polygonCoordinates.getHoles()) {
            encodeLinearRing(ring, writer);
        }
        writer.writeEndArray();
    }

    private static void encodeLinearRing(final List<Position> ring, final BsonWriter writer) {
        writer.writeStartArray();
        for (Position position : ring) {
            encodePosition(writer, position);
        }
        writer.writeEndArray();
    }


    static void encodePosition(final BsonWriter writer, final Position value) {
        writer.writeStartArray();

        for (double number : value.getValues()) {
            writer.writeDouble(number);
        }

        writer.writeEndArray();
    }


    private GeometryCodecHelper() {
    }
}
