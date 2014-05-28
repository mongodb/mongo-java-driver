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

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings(value = {"unchecked", "rawtypes"})
public class GeoJsonMultiPoint extends GeoJsonGeometry<GeoJsonCoordinates> {
    private static final long serialVersionUID = -645193303936135327L;

    private List<List<Double>> pointList =  new ArrayList<List<Double>>();

    {
        this.setType(GeoJsonObject.MULTI_POINT);
    }

    private void init(final List<GeoJsonCoordinates> coordinates) {
        this.setCoordinates(coordinates);
        for (GeoJsonCoordinates coord : coordinates) {
            pointList.add(coord.get());
        }
        this.append(FIELD_TYPE, this.getType());
        this.append(FIELD_COORDINATES , this.pointList);
    }

    public GeoJsonMultiPoint(final List<GeoJsonCoordinates> coordinates) {
        init(coordinates);
    }


    public GeoJsonMultiPoint(final GeoJsonCoordinates...points) {
        List<GeoJsonCoordinates> geoJsonCoordinatesList = new ArrayList<GeoJsonCoordinates>();
        for (GeoJsonCoordinates point : points) {
            geoJsonCoordinatesList.add(point);
        }
        init(geoJsonCoordinatesList);
    }

}
