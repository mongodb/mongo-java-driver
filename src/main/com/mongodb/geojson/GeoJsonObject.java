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

package com.mongodb.geojson;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import java.util.List;

/**
 * Created by tgrall on 5/3/14.
 */
public abstract class GeoJsonObject<T> {


    protected String type;

    public final static String TYPE = "type";
    public final static String COORDINATES = "coordinates";
    public final static String POINT = "Point";
    public final static String POLYGON = "Polygon";
    public final static String LINE_STRING = "LineString";
    public final static String MULTI_POINT = "MultiPoint";
    public final static String MULTI_LINE_STRING = "MultiLineString";
    public final static String MULTI_POLYGON = "MultiPolygon";



    public abstract List getCoordinates();

    public final String getType() {
        return type;
    }

    public DBObject get() {
        DBObject dbo = BasicDBObjectBuilder.start()
                        .add(TYPE, type)
                        .add(COORDINATES, getCoordinates())
                        .get();
        return dbo;
    }

    public String toString() {
        return this.get().toString();
    }

}
