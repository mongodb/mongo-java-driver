/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client.model.geojson;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A representation of a GeoJSON MultiLineString.
 *
 * @since 3.1
 */
public final class MultiLineString extends Geometry {

    private final List<List<Position>> coordinates;

    /**
     * Construct an instance with the given coordinates.
     *
     * @param coordinates the coordinates of each line
     */
    public MultiLineString(final List<List<Position>> coordinates) {
        this(null, coordinates);
    }

    /**
     * Construct an instance with the given coordinates and coordinate reference system.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinates the coordinates of each line
     */
    public MultiLineString(final CoordinateReferenceSystem coordinateReferenceSystem, final List<List<Position>> coordinates) {
        super(coordinateReferenceSystem);

        notNull("coordinates", coordinates);

        for (List<Position> line : coordinates) {
            notNull("line", line);
            isTrueArgument("line contains only non-null positions", !line.contains(null));
        }

        this.coordinates = Collections.unmodifiableList(coordinates);
    }

    @Override
    public GeoJsonObjectType getType() {
        return GeoJsonObjectType.MULTI_LINE_STRING;
    }

    /**
     * Gets the GeoJSON coordinates of this MultiLineString
     *
     * @return the coordinates
     */
    public List<List<Position>> getCoordinates() {
        return coordinates;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        MultiLineString polygon = (MultiLineString) o;

        if (!coordinates.equals(polygon.coordinates)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + coordinates.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MultiLineString{"
               + "coordinates=" + coordinates
               + ((getCoordinateReferenceSystem() == null) ? "" : ", coordinateReferenceSystem=" + getCoordinateReferenceSystem())
               + '}';
    }
}
