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

public class GeoJsonPolygon extends GeoJsonGeometry<List<GeoJsonCoordinates>> {
    List<List<Double>> exteriorRingValues = new ArrayList<>();
    List<List<Double>> interiorRingValues = new ArrayList<>();
    List<List<List<Double>>> pointList =  new ArrayList<>();

    {
        type = GeoJsonObjectType.POLYGON;
    }

    private void init(List<GeoJsonCoordinates> exteriorRing , List<GeoJsonCoordinates> interiorRing) {
        coordinates.add(exteriorRing);
        for(GeoJsonCoordinates coord : exteriorRing) {
            exteriorRingValues.add( coord.get() );
        }
        pointList.add(exteriorRingValues);

        if ( interiorRing !=null) {
            for (GeoJsonCoordinates coord : interiorRing) {
                interiorRingValues.add(coord.get());
            }
            pointList.add(interiorRingValues);
        }

        this.append(FIELD_TYPE, type);
        this.append( FIELD_COORDINATES , this.pointList);
    }


    public GeoJsonPolygon( List<GeoJsonCoordinates> exteriorRing ) {
        init(exteriorRing, null);
    }

    public GeoJsonPolygon(GeoJsonCoordinates...points) {
        List<GeoJsonCoordinates> exteriorRing = new ArrayList<GeoJsonCoordinates>();
        for (GeoJsonCoordinates point : points ) {
            exteriorRing.add(point);
        }

        init(exteriorRing, null);
    }


    public GeoJsonPolygon( List<GeoJsonCoordinates> exteriorRing , List<GeoJsonCoordinates> interiorRing ) {
        init(exteriorRing, interiorRing);
    }

    public List<List<List<Double>>> getPointList() {
        return pointList;
    }

    public List<List<Double>> getExteriorRingValues() {
        return exteriorRingValues;
    }

    public List<List<Double>> getInteriorRingValues() {
        return interiorRingValues;
    }
}


