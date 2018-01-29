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

package com.mongodb.client.model.geojson

import spock.lang.Specification

import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING


class PointSpecification extends Specification {
    def 'constructor should set coordinates'() {
        expect:
        new Point(new Position(1.0d, 2.0d)).coordinates == new Position(1.0d, 2.0d)
        new Point(new Position(1.0d, 2.0d)).position == new Position(1.0d, 2.0d)
    }

    def 'constructor should set coordinate reference system'() {
        expect:
        new Point(new Position(1.0d, 2.0d)).coordinateReferenceSystem == null
        new Point(EPSG_4326_STRICT_WINDING, new Position(1.0d, 2.0d)).coordinateReferenceSystem == EPSG_4326_STRICT_WINDING
    }

    def 'constructors should throw if preconditions are violated'() {
        when:
        new Point(null)

        then:
        thrown(IllegalArgumentException)

        when:
        new Point(EPSG_4326_STRICT_WINDING, null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should get type'() {
        expect:
        new Point(new Position(1.0d, 2.0d)).type == GeoJsonObjectType.POINT
    }

    def 'equals, hashcode and toString should be overridden'() {
        expect:
        new Point(new Position(1.0d, 2.0d)) == new Point(new Position(1.0d, 2.0d))
        new Point(new Position(1.0d, 2.0d)).hashCode() == new Point(new Position(1.0d, 2.0d)).hashCode()
        new Point(new Position(1.0d, 2.0d)).toString() == 'Point{coordinate=Position{values=[1.0, 2.0]}}'
    }
}
