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

import java.util.List;

public class GeoJsonPoint extends GeoJsonObject<GeoJsonCoordinates> {

    {
        type = GeoJsonObjectType.POINT;
    }

    public GeoJsonPoint(GeoJsonCoordinates coordinates) {
        this.coordinates = coordinates;
        this.append( FIELD_TYPE , type  );
        this.append( FIELD_COORDINATES , this.getCoordinates() );
    }

    @Override
    public List<Double> getCoordinates() {
        return coordinates.get();
    }

    public double getLongitude() {
        return coordinates.getLongitude();
    }

    public double getLatitude() {
        return coordinates.getLatitude();
    }

}
