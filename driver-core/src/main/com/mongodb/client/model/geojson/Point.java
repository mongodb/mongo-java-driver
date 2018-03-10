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

import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A representation of a GeoJSON Point.
 *
 * @since 3.1
 */
public final class Point extends Geometry {
    private final Position coordinate;

    /**
     * Construct an instance with the given coordinate.
     *
     * @param coordinate the non-null coordinate of the point
     */
    public Point(final Position coordinate) {
        this(null, coordinate);
    }

    /**
     * Construct an instance with the given coordinate and coordinate reference system.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinate the non-null coordinate of the point
     */
    public Point(@Nullable final CoordinateReferenceSystem coordinateReferenceSystem, final Position coordinate) {
        super(coordinateReferenceSystem);
        this.coordinate = notNull("coordinates", coordinate);
    }

    @Override
    public GeoJsonObjectType getType() {
        return GeoJsonObjectType.POINT;
    }

    /**
     * Gets the GeoJSON coordinates of this point.
     *
     * @return the coordinates
     */
    public Position getCoordinates() {
        return coordinate;
    }

    /**
     * Gets the position of this point.
     *
     * @return the position
     */
    public Position getPosition(){
        return coordinate;
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

        Point point = (Point) o;

        if (!coordinate.equals(point.coordinate)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        return 31 * result + coordinate.hashCode();
    }

    @Override
    public String toString() {
        return "Point{"
               + "coordinate=" + coordinate
               + ((getCoordinateReferenceSystem() == null) ? "" : ", coordinateReferenceSystem=" + getCoordinateReferenceSystem())
               + '}';
    }
}
