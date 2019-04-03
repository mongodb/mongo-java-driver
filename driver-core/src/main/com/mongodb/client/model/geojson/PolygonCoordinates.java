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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * Coordinates for a GeoJSON Polygon.
 *
 * @since 3.1
 */
public final class PolygonCoordinates {
    private final List<Position> exterior;
    private final List<List<Position>> holes;

    /**
     * Construct an instance.
     *
     * @param exterior the exterior ring of the polygon
     * @param holes    optional interior rings of the polygon
     */
    @SafeVarargs
    public PolygonCoordinates(final List<Position> exterior, final List<Position>... holes) {
        notNull("exteriorRing", exterior);
        isTrueArgument("ring contains only non-null positions", !exterior.contains(null));
        isTrueArgument("ring must contain at least four positions", exterior.size() >= 4);
        isTrueArgument("first and last position must be the same", exterior.get(0).equals(exterior.get(exterior.size() - 1)));

        this.exterior = Collections.unmodifiableList(exterior);

        List<List<Position>> holesList = new ArrayList<List<Position>>(holes.length);
        for (List<Position> hole : holes) {
            notNull("interiorRing", hole);
            isTrueArgument("ring contains only non-null positions", !hole.contains(null));
            isTrueArgument("ring must contain at least four positions", hole.size() >= 4);
            isTrueArgument("first and last position must be the same", hole.get(0).equals(hole.get(hole.size() - 1)));
            holesList.add(Collections.unmodifiableList(hole));
        }

        this.holes = Collections.unmodifiableList(holesList);
    }

    /**
     * Gets the exterior of the polygon.
     *
     * @return the exterior of the polygon
     */
    public List<Position> getExterior() {
        return exterior;
    }

    /**
     * Gets the holes in the polygon.
     *
     * @return the holes in the polygon, which will not be null but may be empty
     */
    public List<List<Position>> getHoles() {
        return holes;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PolygonCoordinates that = (PolygonCoordinates) o;

        if (!exterior.equals(that.exterior)) {
            return false;
        }
        if (!holes.equals(that.holes)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = exterior.hashCode();
        result = 31 * result + holes.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PolygonCoordinates{"
               + "exterior=" + exterior
               + (holes.isEmpty() ? "" : ", holes=" + holes)
               + '}';
    }
}
