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

public class GeoJson {

    public static GeoJson2DCoordinates position(double x, double y) {
        return new GeoJson2DCoordinates(x,y);
    }

    public static GeoJson3DCoordinates position(double x, double y, double z) {
        return new GeoJson3DCoordinates(x, y, z);
    }

}
