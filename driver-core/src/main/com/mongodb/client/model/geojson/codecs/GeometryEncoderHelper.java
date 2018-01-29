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
import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;

import static java.lang.String.format;

final class GeometryEncoderHelper {


    @SuppressWarnings("unchecked")
    static void encodeGeometry(final BsonWriter writer, final Geometry value, final EncoderContext encoderContext,
                               final CodecRegistry registry) {

        writer.writeStartDocument();
        writer.writeString("type", value.getType().getTypeName());

        if (value instanceof GeometryCollection) {
            writer.writeName("geometries");
            encodeGeometryCollection(writer, (GeometryCollection) value, encoderContext, registry);
        } else {
            writer.writeName("coordinates");
            if (value instanceof Point) {
                encodePoint(writer, (Point) value);
            } else if (value instanceof MultiPoint) {
                encodeMultiPoint(writer, (MultiPoint) value);
            } else if (value instanceof Polygon) {
                encodePolygon(writer, (Polygon) value);
            } else if (value instanceof MultiPolygon) {
                encodeMultiPolygon(writer, (MultiPolygon) value);
            } else if (value instanceof LineString) {
                encodeLineString(writer, (LineString) value);
            } else if (value instanceof MultiLineString) {
                encodeMultiLineString(writer, (MultiLineString) value);
            } else {
                throw new CodecConfigurationException(format("Unsupported Geometry: %s", value));
            }
        }

        encodeCoordinateReferenceSystem(writer, value, encoderContext, registry);
        writer.writeEndDocument();
    }

    private static void encodePoint(final BsonWriter writer, final Point value) {
        encodePosition(writer, value.getPosition());
    }

    private static void encodeMultiPoint(final BsonWriter writer, final MultiPoint value) {
        writer.writeStartArray();
        for (Position position : value.getCoordinates()) {
            encodePosition(writer, position);
        }
        writer.writeEndArray();
    }

    private static void encodePolygon(final BsonWriter writer, final Polygon value) {
        encodePolygonCoordinates(writer, value.getCoordinates());
    }

    private static void encodeMultiPolygon(final BsonWriter writer, final MultiPolygon value) {
        writer.writeStartArray();
        for (PolygonCoordinates polygonCoordinates : value.getCoordinates()) {
            encodePolygonCoordinates(writer, polygonCoordinates);
        }
        writer.writeEndArray();
    }

    private static void encodeLineString(final BsonWriter writer, final LineString value) {
        writer.writeStartArray();
        for (Position position : value.getCoordinates()) {
            encodePosition(writer, position);
        }
        writer.writeEndArray();
    }

    private static void encodeMultiLineString(final BsonWriter writer, final MultiLineString value) {
        writer.writeStartArray();
        for (List<Position> ring : value.getCoordinates()) {
            writer.writeStartArray();
            for (Position position : ring) {
                encodePosition(writer, position);
            }
            writer.writeEndArray();
        }
        writer.writeEndArray();
    }

    private static void encodeGeometryCollection(final BsonWriter writer, final GeometryCollection value,
                                                 final EncoderContext encoderContext, final CodecRegistry registry) {
        writer.writeStartArray();
        for (Geometry geometry : value.getGeometries()) {
            encodeGeometry(writer, geometry, encoderContext, registry);
        }
        writer.writeEndArray();
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

    private GeometryEncoderHelper() {
    }
}
