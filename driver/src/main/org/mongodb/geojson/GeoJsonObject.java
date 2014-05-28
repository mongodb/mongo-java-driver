/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb.geojson;


import org.mongodb.Document;

public abstract class GeoJsonObject<T> extends Document{
    private static final long serialVersionUID = 7054387876653705497L;

    public static final String FEATURE = "Feature";

    public static final String FEATURE_COLLECTION = "FeatureCollection";

    public static final String GEOMETRY_COLLECTION = "GeometryCollection";

    public static final String LINE_STRING = "LineString";

    public static final String MULTI_LINE_STRING = "MultiLineString";

    public static final String MULTI_POINT = "MultiPoint";

    public static final String MULTI_POLYGON = "MultiPolygon";

    public static final String POINT = "Point";

    public static final String POLYGON = "Polygon";


    protected static final String FIELD_TYPE = "type";
    protected static final String FIELD_COORDINATES = "coordinates";
    protected static final String FIELD_GEOMETRIES = "geometries";

    private String type;
    private T coordinates;

    public T getCoordinates() {
        return this.coordinates;
    }

    public void setCoordinates(final T coordinates) {
        this.coordinates = coordinates;
    }

    public String getType() {
        return type;
    }

    protected void setType(final String type) {
        this.type = type;
    }

    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}