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
import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * A provider of codecs for GeoJSON objects.
 *
 * @since 3.1
 */
public class GeoJsonCodecProvider implements CodecProvider {
    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (clazz.equals(Polygon.class)) {
            return (Codec<T>) new PolygonCodec(registry);
        } else if (clazz.equals(Point.class)) {
            return (Codec<T>) new PointCodec(registry);
        } else if (clazz.equals(LineString.class)) {
            return (Codec<T>) new LineStringCodec(registry);
        } else if (clazz.equals(MultiPoint.class)) {
            return (Codec<T>) new MultiPointCodec(registry);
        } else if (clazz.equals(MultiLineString.class)) {
            return (Codec<T>) new MultiLineStringCodec(registry);
        } else if (clazz.equals(MultiPolygon.class)) {
            return (Codec<T>) new MultiPolygonCodec(registry);
        } else if (clazz.equals(GeometryCollection.class)) {
            return (Codec<T>) new GeometryCollectionCodec(registry);
        } else if (clazz.equals(NamedCoordinateReferenceSystem.class)) {
            return (Codec<T>) new NamedCoordinateReferenceSystemCodec();
        } else if (clazz.equals(Geometry.class)) {
            return (Codec<T>) new GeometryCodec(registry);
        }

        return null;
    }
}
