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

import com.mongodb.client.model.geojson.CoordinateReferenceSystem;
import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;
import com.mongodb.lang.Nullable;
import org.bson.BsonReader;
import org.bson.BsonReaderMark;
import org.bson.BsonType;
import org.bson.codecs.configuration.CodecConfigurationException;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

final class GeometryDecoderHelper {

    @SuppressWarnings("unchecked")
    static <T extends Geometry> T decodeGeometry(final BsonReader reader, final Class<T> clazz) {
        if (clazz.equals(Point.class)) {
            return (T) decodePoint(reader);
        } else if (clazz.equals(MultiPoint.class)) {
            return (T) decodeMultiPoint(reader);
        } else if (clazz.equals(Polygon.class)) {
            return (T) decodePolygon(reader);
        } else if (clazz.equals(MultiPolygon.class)) {
            return (T) decodeMultiPolygon(reader);
        } else if (clazz.equals(LineString.class)) {
            return (T) decodeLineString(reader);
        } else if (clazz.equals(MultiLineString.class)) {
            return (T) decodeMultiLineString(reader);
        } else if (clazz.equals(GeometryCollection.class)) {
            return (T) decodeGeometryCollection(reader);
        } else if (clazz.equals(Geometry.class)) {
            return (T) decodeGeometry(reader);
        }

        throw new CodecConfigurationException(format("Unsupported Geometry: %s", clazz));
    }

    private static Point decodePoint(final BsonReader reader) {
        String type = null;
        Position position = null;
        CoordinateReferenceSystem crs = null;
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            if (key.equals("type")) {
                type = reader.readString();
            } else if (key.equals("coordinates")) {
                position = decodePosition(reader);
            } else if (key.equals("crs")) {
                crs = decodeCoordinateReferenceSystem(reader);
            } else {
                throw new CodecConfigurationException(format("Unexpected key '%s' found when decoding a GeoJSON point", key));
            }
        }
        reader.readEndDocument();

        if (type == null) {
            throw new CodecConfigurationException("Invalid Point, document contained no type information.");
        } else if (!type.equals("Point")) {
            throw new CodecConfigurationException(format("Invalid Point, found type '%s'.", type));
        } else if (position == null) {
            throw new CodecConfigurationException("Invalid Point, missing position coordinates.");
        }
        return crs != null ? new Point(crs, position) : new Point(position);
    }

    private static MultiPoint decodeMultiPoint(final BsonReader reader) {
        String type = null;
        List<Position> coordinates = null;
        CoordinateReferenceSystem crs = null;
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            if (key.equals("type")) {
                type = reader.readString();
            } else if (key.equals("coordinates")) {
                coordinates = decodeCoordinates(reader);
            } else if (key.equals("crs")) {
                crs = decodeCoordinateReferenceSystem(reader);
            } else {
                throw new CodecConfigurationException(format("Unexpected key '%s' found when decoding a GeoJSON point", key));
            }
        }
        reader.readEndDocument();

        if (type == null) {
            throw new CodecConfigurationException("Invalid MultiPoint, document contained no type information.");
        } else if (!type.equals("MultiPoint")) {
            throw new CodecConfigurationException(format("Invalid MultiPoint, found type '%s'.", type));
        } else if (coordinates == null) {
            throw new CodecConfigurationException("Invalid MultiPoint, missing position coordinates.");
        }
        return crs != null ? new MultiPoint(crs, coordinates) : new MultiPoint(coordinates);
    }

    private static Polygon decodePolygon(final BsonReader reader) {
        String type = null;
        PolygonCoordinates coordinates = null;
        CoordinateReferenceSystem crs = null;

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            if (key.equals("type")) {
                type = reader.readString();
            } else if (key.equals("coordinates")) {
                coordinates = decodePolygonCoordinates(reader);
            } else if (key.equals("crs")) {
                crs = decodeCoordinateReferenceSystem(reader);
            } else {
                throw new CodecConfigurationException(format("Unexpected key '%s' found when decoding a GeoJSON Polygon", key));
            }
        }
        reader.readEndDocument();

        if (type == null) {
            throw new CodecConfigurationException("Invalid Polygon, document contained no type information.");
        } else if (!type.equals("Polygon")) {
            throw new CodecConfigurationException(format("Invalid Polygon, found type '%s'.", type));
        } else if (coordinates == null) {
            throw new CodecConfigurationException("Invalid Polygon, missing coordinates.");
        }
        return crs != null ? new Polygon(crs, coordinates) : new Polygon(coordinates);
    }

    private static MultiPolygon decodeMultiPolygon(final BsonReader reader) {
        String type = null;
        List<PolygonCoordinates> coordinates = null;
        CoordinateReferenceSystem crs = null;

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            if (key.equals("type")) {
                type = reader.readString();
            } else if (key.equals("coordinates")) {
                coordinates = decodeMultiPolygonCoordinates(reader);
            } else if (key.equals("crs")) {
                crs = decodeCoordinateReferenceSystem(reader);
            } else {
                throw new CodecConfigurationException(format("Unexpected key '%s' found when decoding a GeoJSON Polygon", key));
            }
        }
        reader.readEndDocument();

        if (type == null) {
            throw new CodecConfigurationException("Invalid MultiPolygon, document contained no type information.");
        } else if (!type.equals("MultiPolygon")) {
            throw new CodecConfigurationException(format("Invalid MultiPolygon, found type '%s'.", type));
        } else if (coordinates == null) {
            throw new CodecConfigurationException("Invalid MultiPolygon, missing coordinates.");
        }
        return crs != null ? new MultiPolygon(crs, coordinates) : new MultiPolygon(coordinates);
    }

    private static LineString decodeLineString(final BsonReader reader) {
        String type = null;
        List<Position> coordinates = null;
        CoordinateReferenceSystem crs = null;

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            if (key.equals("type")) {
                type = reader.readString();
            } else if (key.equals("coordinates")) {
                coordinates = decodeCoordinates(reader);
            } else if (key.equals("crs")) {
                crs = decodeCoordinateReferenceSystem(reader);
            } else {
                throw new CodecConfigurationException(format("Unexpected key '%s' found when decoding a GeoJSON Polygon", key));
            }
        }
        reader.readEndDocument();

        if (type == null) {
            throw new CodecConfigurationException("Invalid LineString, document contained no type information.");
        } else if (!type.equals("LineString")) {
            throw new CodecConfigurationException(format("Invalid LineString, found type '%s'.", type));
        } else if (coordinates == null) {
            throw new CodecConfigurationException("Invalid LineString, missing coordinates.");
        }
        return crs != null ? new LineString(crs, coordinates) : new LineString(coordinates);
    }

    private static MultiLineString decodeMultiLineString(final BsonReader reader) {
        String type = null;
        List<List<Position>> coordinates = null;
        CoordinateReferenceSystem crs = null;

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            if (key.equals("type")) {
                type = reader.readString();
            } else if (key.equals("coordinates")) {
                coordinates = decodeMultiCoordinates(reader);
            } else if (key.equals("crs")) {
                crs = decodeCoordinateReferenceSystem(reader);
            } else {
                throw new CodecConfigurationException(format("Unexpected key '%s' found when decoding a GeoJSON Polygon", key));
            }
        }
        reader.readEndDocument();

        if (type == null) {
            throw new CodecConfigurationException("Invalid MultiLineString, document contained no type information.");
        } else if (!type.equals("MultiLineString")) {
            throw new CodecConfigurationException(format("Invalid MultiLineString, found type '%s'.", type));
        } else if (coordinates == null) {
            throw new CodecConfigurationException("Invalid MultiLineString, missing coordinates.");
        }
        return crs != null ? new MultiLineString(crs, coordinates) : new MultiLineString(coordinates);
    }

    private static GeometryCollection decodeGeometryCollection(final BsonReader reader) {
        String type = null;
        List<? extends Geometry> geometries = null;
        CoordinateReferenceSystem crs = null;

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            if (key.equals("type")) {
                type = reader.readString();
            } else if (key.equals("geometries")) {
                geometries = decodeGeometries(reader);
            } else if (key.equals("crs")) {
                crs = decodeCoordinateReferenceSystem(reader);
            } else {
                throw new CodecConfigurationException(format("Unexpected key '%s' found when decoding a GeoJSON Polygon", key));
            }
        }
        reader.readEndDocument();

        if (type == null) {
            throw new CodecConfigurationException("Invalid GeometryCollection, document contained no type information.");
        } else if (!type.equals("GeometryCollection")) {
            throw new CodecConfigurationException(format("Invalid GeometryCollection, found type '%s'.", type));
        } else if (geometries == null) {
            throw new CodecConfigurationException("Invalid GeometryCollection, missing geometries.");
        }
        return crs != null ? new GeometryCollection(crs, geometries) : new GeometryCollection(geometries);
    }

    private static List<? extends Geometry> decodeGeometries(final BsonReader reader) {
        validateIsArray(reader);
        reader.readStartArray();
        List<Geometry> values = new ArrayList<Geometry>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            Geometry geometry = decodeGeometry(reader);
            values.add(geometry);
        }
        reader.readEndArray();


        return values;
    }

    private static Geometry decodeGeometry(final BsonReader reader) {
        String type = null;
        BsonReaderMark mark = reader.getMark();
        validateIsDocument(reader);
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            if (key.equals("type")) {
                type = reader.readString();
                break;
            } else {
                reader.skipValue();
            }
        }
        mark.reset();

        if (type == null) {
            throw new CodecConfigurationException("Invalid Geometry item, document contained no type information.");
        }
        Geometry geometry = null;
        if (type.equals("Point")) {
            geometry = decodePoint(reader);
        } else if (type.equals("MultiPoint")) {
            geometry = decodeMultiPoint(reader);
        } else if (type.equals("Polygon")) {
            geometry = decodePolygon(reader);
        } else if (type.equals("MultiPolygon")) {
            geometry = decodeMultiPolygon(reader);
        } else if (type.equals("LineString")) {
            geometry = decodeLineString(reader);
        } else if (type.equals("MultiLineString")) {
            geometry = decodeMultiLineString(reader);
        } else if (type.equals("GeometryCollection")) {
            geometry = decodeGeometryCollection(reader);
        } else {
            throw new CodecConfigurationException(format("Invalid Geometry item, found type '%s'.", type));
        }
        return geometry;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static PolygonCoordinates decodePolygonCoordinates(final BsonReader reader) {
        validateIsArray(reader);
        reader.readStartArray();
        List<List<Position>> values = new ArrayList<List<Position>>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            values.add(decodeCoordinates(reader));
        }
        reader.readEndArray();

        if (values.isEmpty()) {
            throw new CodecConfigurationException("Invalid Polygon no coordinates.");
        }

        List<Position> exterior = values.remove(0);
        ArrayList[] holes = values.toArray(new ArrayList[values.size()]);

        try {
            return new PolygonCoordinates(exterior, holes);
        } catch (IllegalArgumentException e) {
            throw new CodecConfigurationException(format("Invalid Polygon: %s", e.getMessage()));
        }
    }

    private static List<PolygonCoordinates> decodeMultiPolygonCoordinates(final BsonReader reader) {
        validateIsArray(reader);
        reader.readStartArray();
        List<PolygonCoordinates> values = new ArrayList<PolygonCoordinates>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            values.add(decodePolygonCoordinates(reader));
        }
        reader.readEndArray();

        if (values.isEmpty()) {
            throw new CodecConfigurationException("Invalid MultiPolygon no coordinates.");
        }
        return values;
    }

    private static List<Position> decodeCoordinates(final BsonReader reader) {
        validateIsArray(reader);
        reader.readStartArray();
        List<Position> values = new ArrayList<Position>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            values.add(decodePosition(reader));
        }
        reader.readEndArray();
        return values;
    }

    private static List<List<Position>> decodeMultiCoordinates(final BsonReader reader) {
        validateIsArray(reader);
        reader.readStartArray();
        List<List<Position>> values = new ArrayList<List<Position>>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            values.add(decodeCoordinates(reader));
        }
        reader.readEndArray();
        return values;
    }

    private static Position decodePosition(final BsonReader reader) {
        validateIsArray(reader);
        reader.readStartArray();
        List<Double> values = new ArrayList<Double>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            values.add(readAsDouble(reader));
        }
        reader.readEndArray();

        try {
            return new Position(values);
        } catch (IllegalArgumentException e) {
            throw new CodecConfigurationException(format("Invalid Position: %s", e.getMessage()));
        }
    }

    private static double readAsDouble(final BsonReader reader) {
        if (reader.getCurrentBsonType() == BsonType.DOUBLE) {
           return reader.readDouble();
        } else if (reader.getCurrentBsonType() == BsonType.INT32) {
            return reader.readInt32();
        } else if (reader.getCurrentBsonType() == BsonType.INT64) {
            return reader.readInt64();
        }

        throw new CodecConfigurationException("A GeoJSON position value must be a numerical type, but the value is of type "
                + reader.getCurrentBsonType());
    }

    @Nullable
    static CoordinateReferenceSystem decodeCoordinateReferenceSystem(final BsonReader reader) {
        String crsName = null;
        validateIsDocument(reader);
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String name = reader.readName();
            if (name.equals("type")) {
                String type = reader.readString();
                if (!type.equals("name")) {
                    throw new CodecConfigurationException(format("Unsupported CoordinateReferenceSystem '%s'.", type));
                }
            } else if (name.equals("properties")) {
                crsName = decodeCoordinateReferenceSystemProperties(reader);
            } else {
                throw new CodecConfigurationException(format("Found invalid key '%s' in the CoordinateReferenceSystem.", name));
            }
        }
        reader.readEndDocument();

        return crsName != null ? new NamedCoordinateReferenceSystem(crsName) : null;
    }

    private static String decodeCoordinateReferenceSystemProperties(final BsonReader reader) {
        String crsName = null;
        validateIsDocument(reader);
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String name = reader.readName();
            if (name.equals("name")) {
                crsName = reader.readString();
            } else {
                throw new CodecConfigurationException(format("Found invalid key '%s' in the CoordinateReferenceSystem.", name));
            }
        }
        reader.readEndDocument();

        if (crsName == null) {
            throw new CodecConfigurationException("Found invalid properties in the CoordinateReferenceSystem.");
        }
        return crsName;
    }

    private static void validateIsDocument(final BsonReader reader) {
        BsonType currentType = reader.getCurrentBsonType();
        if (currentType == null) {
            currentType = reader.readBsonType();
        }
        if (!currentType.equals(BsonType.DOCUMENT)) {
            throw new CodecConfigurationException("Invalid BsonType expecting a Document");
        }
    }

    private static void validateIsArray(final BsonReader reader) {
        if (reader.getCurrentBsonType() != BsonType.ARRAY) {
            throw new CodecConfigurationException("Invalid BsonType expecting an Array");
        }
    }

    private GeometryDecoderHelper() {
    }
}
