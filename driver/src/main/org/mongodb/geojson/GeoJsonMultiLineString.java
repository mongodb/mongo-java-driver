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
public class GeoJsonMultiLineString extends GeoJsonGeometry<List<GeoJsonCoordinates>> {
    private static final long serialVersionUID = -5936466663766444883L;

    private List<List<List<Double>>> allLineValues = new ArrayList<List<List<Double>>>();

    {
        this.setType(GeoJsonObject.MULTI_LINE_STRING);
    }

    private void init(final List<List<GeoJsonCoordinates>> allLines) {
        this.setCoordinates(allLines);

        for (List<GeoJsonCoordinates> line : allLines) {

            List<List<Double>> lineValues = new ArrayList<List<Double>>();
            for (GeoJsonCoordinates coord : line) {
                lineValues.add(coord.get());
            }
            allLineValues.add(lineValues);
        }

        this.append(FIELD_TYPE, this.getType());
        this.append(FIELD_COORDINATES, this.allLineValues);

    }

    public GeoJsonMultiLineString(final List<List<GeoJsonCoordinates>> lineString) {
        init(lineString);
    }
}
