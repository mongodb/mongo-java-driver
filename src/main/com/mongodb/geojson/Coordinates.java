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

import java.util.ArrayList;

/**
 * Longitude, Latitude (GeoJson coordinates)
 * Currently this implementation does not support altitude
 *
 * TODO : find the correct implementation
 */
public class Coordinates extends ArrayList<Double> {

    private double longitude;
    private double latitude;

    public Coordinates(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
        super.add(longitude);
        super.add(latitude);
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }


}
