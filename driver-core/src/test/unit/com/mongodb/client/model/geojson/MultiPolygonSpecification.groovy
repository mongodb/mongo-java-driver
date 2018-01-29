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


class MultiPolygonSpecification extends Specification {
    def exteriorOne = [new Position([40.0d, 18.0d]),
                       new Position([40.0d, 19.0d]),
                       new Position([41.0d, 19.0d]),
                       new Position([40.0d, 18.0d])]
    def coordinatesOne = new PolygonCoordinates(exteriorOne)

    def exteriorTwo = [new Position([80.0d, 18.0d]),
                       new Position([80.0d, 19.0d]),
                       new Position([81.0d, 19.0d]),
                       new Position([80.0d, 18.0d])]
    def coordinatesTwo = new PolygonCoordinates(exteriorTwo)

    def 'constructor should set coordinates'() {
        expect:
        new MultiPolygon([coordinatesOne, coordinatesTwo]).coordinates == [coordinatesOne, coordinatesTwo]
    }

    def 'constructor should set coordinate reference system'() {
        expect:
        new MultiPolygon([coordinatesOne]).coordinateReferenceSystem == null
        new MultiPolygon(EPSG_4326_STRICT_WINDING, [coordinatesOne]).coordinateReferenceSystem == EPSG_4326_STRICT_WINDING
    }

    def 'constructors should throw if preconditions are violated'() {
        when:
        new MultiPolygon(null)

        then:
        thrown(IllegalArgumentException)

        when:
        new MultiPolygon([coordinatesOne, null])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should get type'() {
        expect:
        new MultiPolygon([coordinatesOne]).type == GeoJsonObjectType.MULTI_POLYGON
    }

    def 'equals, hashcode and toString should be overridden'() {
        expect:
        new MultiPolygon([coordinatesOne, coordinatesTwo]) == new MultiPolygon([coordinatesOne, coordinatesTwo])
        new MultiPolygon([coordinatesOne, coordinatesTwo]).hashCode() == new MultiPolygon([coordinatesOne, coordinatesTwo]).hashCode()
        new MultiPolygon([coordinatesOne, coordinatesTwo]).toString() ==
        'MultiPolygon{coordinates=[' +
        'PolygonCoordinates{exterior=[Position{values=[40.0, 18.0]}, Position{values=[40.0, 19.0]}, Position{values=[41.0, 19.0]}, ' +
        'Position{values=[40.0, 18.0]}]}, ' +
        'PolygonCoordinates{exterior=[Position{values=[80.0, 18.0]}, Position{values=[80.0, 19.0]}, Position{values=[81.0, 19.0]}, ' +
        'Position{values=[80.0, 18.0]}]}]}'
    }
}
