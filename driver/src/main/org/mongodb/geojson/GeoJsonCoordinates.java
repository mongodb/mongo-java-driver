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
public  abstract class GeoJsonCoordinates extends ArrayList {
    private static final long serialVersionUID = -8178803264002989655L;

    private Double longitude = null;
    private Double latitude = null;

    private List<Double> coordinatesAsList =  new ArrayList<Double>();

    public List<Double> get(){
        return coordinatesAsList;
    }

    public GeoJsonCoordinates() {
    }

    public GeoJsonCoordinates(final Double longitude, final  Double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
        coordinatesAsList.add(longitude);
        coordinatesAsList.add(latitude);
        // TODO : use a list to serialize is properly in document
        //        get() is not used?
        super.add(longitude);
        super.add(latitude);

    }

    public Double getLongitude() {
        return this.longitude;
    }

    public Double getLatitude() {
        return this.latitude;
    }

    public  List<Double> getCoordinatesAsList(){
        return coordinatesAsList;
    }

    @Override
    public String toString() {
        return "GeoJsonCoordinates{"
               + "longitude=" + longitude
               + ", latitude=" + latitude
               + ", coordinatesAsList=" + coordinatesAsList
               + '}';
    }
}
