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

public class GeoJsonMultiLineString extends GeoJsonGeometry<List<GeoJsonCoordinates>> {

    List<List<List<Double>>> allLineValues = new ArrayList<>();

    {
        type = GeoJsonObjectType.MULTI_LINE_STRING;
    }

    private void init(List<List<GeoJsonCoordinates>> allLines) {
        this.coordinates = allLines;

        for(List<GeoJsonCoordinates> line : allLines) {

            List<List<Double>> lineValues = new ArrayList<>();
            for( GeoJsonCoordinates coord : line  ) {
                lineValues.add( coord.get() );
            }
            allLineValues.add(lineValues);
        }

        this.append(FIELD_TYPE, type);
        this.append(FIELD_COORDINATES, this.allLineValues);

    }

    public GeoJsonMultiLineString(List<List<GeoJsonCoordinates>> lineString) {
        init(lineString);
    }
}
