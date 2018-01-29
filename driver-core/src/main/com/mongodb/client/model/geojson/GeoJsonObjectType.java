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

package com.mongodb.client.model.geojson;

/**
 * An enumeration of GeoJSON object types.
 *
 * @since 3.1
 */
public enum GeoJsonObjectType {
    /**
     *  A GeometryCollection
     */
    GEOMETRY_COLLECTION("GeometryCollection"),

    /**
     *  A LineString
     */
    LINE_STRING("LineString"),

    /**
     *  A MultiLineString
     */
    MULTI_LINE_STRING("MultiLineString"),

    /**
     *  A MultiPoint
     */
    MULTI_POINT("MultiPoint"),

    /**
     *  A MultiPolygon
     */
    MULTI_POLYGON("MultiPolygon"),

    /**
     *  A Point
     */
    POINT("Point"),

    /**
     *  A Polygon
     */
    POLYGON("Polygon");

    /**
     * Gets the GeoJSON-defined name for the object type.
     *
     * @return the GeoJSON-defined type name
     */
    public String getTypeName() {
        return typeName;
    }

    private final String typeName;

    GeoJsonObjectType(final String typeName) {
        this.typeName = typeName;
    }
}
