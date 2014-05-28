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

public interface GeoJsonObjectType {

    public final static String FEATURE = "Feature";

    public final static String FEATURE_COLLECTION = "FeatureCollection";

    public final static String GEOMETRY_COLLECTION = "GeometryCollection";

    public final static String LINE_STRING = "LineString";

    public final static String MULTI_LINE_STRING = "MultiLineString";

    public final static String MULTI_POINT = "MultiPoint";

    public final static String MULTI_POLYGON = "MultiPolygon";

    public final static String POINT = "Point";

    public final static String POLYGON = "Polygon";


}
