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
 * A representation of a GeoJSON GeometryCollection.
 *
 * @since 3.1
 */
public final class GeometryCollection extends Geometry {
    private final List<? extends Geometry> geometries;

    /**
     * Construct an instance with the given list of Geometry objects
     *
     * @param geometries  the list of Geometry objects
     */
    public GeometryCollection(final List<? extends Geometry> geometries) {
        this(null, geometries);
    }

    /**
     * Construct an instance with the given list of Geometry objects
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param geometries  the list of Geometry objects
     */
    public GeometryCollection(final CoordinateReferenceSystem coordinateReferenceSystem,
                              final List<? extends Geometry> geometries) {
        super(coordinateReferenceSystem);
        notNull("geometries", geometries);
        isTrueArgument("geometries contains only non-null elements", !geometries.contains(null));
        this.geometries = Collections.unmodifiableList(geometries);
    }

    @Override
    public GeoJsonObjectType getType() {
        return GeoJsonObjectType.GEOMETRY_COLLECTION;
    }

    /**
     * Gets the list of Geometry objects in this collection.
     *
     * @return the list
     */
    public List<? extends Geometry> getGeometries() {
        return geometries;
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

        GeometryCollection that = (GeometryCollection) o;

        if (!geometries.equals(that.geometries)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + geometries.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "GeometryCollection{"
               + "geometries=" + geometries
               + ((getCoordinateReferenceSystem() == null) ? "" : ", coordinateReferenceSystem=" + getCoordinateReferenceSystem())
               + '}';
    }
}
