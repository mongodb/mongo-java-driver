/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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


class MultiLineStringSpecification extends Specification {
    def coordinates = [[new Position([1.0d, 1.0d]), new Position([2.0d, 2.0d]), new Position([3.0d, 4.0d])],
                       [new Position([2.0d, 3.0d]), new Position([3.0d, 2.0d]), new Position([4.0d, 4.0d])]]

    def 'constructor should set coordinates'() {
        expect:
        new MultiLineString(coordinates).coordinates == coordinates
    }

    def 'constructor should set coordinate reference system'() {
        expect:
        new MultiLineString(coordinates).coordinateReferenceSystem == null
        new MultiLineString(EPSG_4326_STRICT_WINDING, coordinates).coordinateReferenceSystem == EPSG_4326_STRICT_WINDING
    }

    def 'constructors should throw if preconditions are violated'() {
        when:
        new MultiLineString(null)

        then:
        thrown(IllegalArgumentException)

        when:
        new MultiLineString([[new Position([40.0d, 18.0d]), new Position([40.0d, 19.0d])], null])

        then:
        thrown(IllegalArgumentException)

        when:
        new MultiLineString([[new Position([40.0d, 18.0d]), new Position([40.0d, 19.0d]), null]])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should get type'() {
        expect:
        new MultiLineString(coordinates).type == GeoJsonObjectType.MULTI_LINE_STRING
    }

    def 'equals, hashcode and toString should be overridden'() {
        expect:
        new MultiLineString(coordinates) == new MultiLineString(coordinates)
        new MultiLineString(coordinates).hashCode() == new MultiLineString(coordinates).hashCode()
        new MultiLineString(coordinates).toString() ==
        'MultiLineString{coordinates=[' +
        '[Position{values=[1.0, 1.0]}, Position{values=[2.0, 2.0]}, Position{values=[3.0, 4.0]}], ' +
        '[Position{values=[2.0, 3.0]}, Position{values=[3.0, 2.0]}, Position{values=[4.0, 4.0]}]]}'
    }
}
