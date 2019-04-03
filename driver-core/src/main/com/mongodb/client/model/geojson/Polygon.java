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

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A representation of a GeoJSON Polygon.
 *
 * @since 3.1
 */
public final class Polygon extends Geometry {

    private final PolygonCoordinates coordinates;

    /**
     * Construct an instance with the given coordinates.
     *
     * @param exterior the exterior ring of the polygon
     * @param holes    optional interior rings of the polygon
     */
    @SafeVarargs
    public Polygon(final List<Position> exterior, final List<Position>... holes) {
        this(new PolygonCoordinates(exterior, holes));
    }

    /**
     * Construct an instance with the given coordinates.
     *
     * @param coordinates the coordinates
     */
    public Polygon(final PolygonCoordinates coordinates) {
        this(null, coordinates);
    }

    /**
     * Construct an instance with the given coordinates and coordinate reference system.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinates               the coordinates
     */
    public Polygon(@Nullable final CoordinateReferenceSystem coordinateReferenceSystem, final PolygonCoordinates coordinates) {
        super(coordinateReferenceSystem);
        this.coordinates = notNull("coordinates", coordinates);
    }

    @Override
    public GeoJsonObjectType getType() {
        return GeoJsonObjectType.POLYGON;
    }

    /**
     * Gets the GeoJSON coordinates of the polygon
     *
     * @return the coordinates, which must have at least one element
     */
    public PolygonCoordinates getCoordinates() {
        return coordinates;
    }

    /**
     * Gets the exterior coordinates.
     *
     * @return the exterior coordinates
     */
    public List<Position> getExterior() {
        return coordinates.getExterior();
    }

    /**
     * Get the holes in this polygon.
     *
     * @return the possibly-empty list of holes
     */
    public List<List<Position>> getHoles() {
        return coordinates.getHoles();
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

        Polygon polygon = (Polygon) o;

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
        return "Polygon{"
               + "exterior=" + coordinates.getExterior()
               + (coordinates.getHoles().isEmpty() ? "" : ", holes=" + coordinates.getHoles())
               + ((getCoordinateReferenceSystem() == null) ? "" : ", coordinateReferenceSystem=" + getCoordinateReferenceSystem())
               + '}';
    }
}
