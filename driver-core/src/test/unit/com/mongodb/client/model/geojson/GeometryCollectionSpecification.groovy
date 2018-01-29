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

import groovy.transform.CompileStatic
import spock.lang.Specification

class GeometryCollectionSpecification extends Specification {
    def geometries = [new Point(new Position(1d, 2d)), new Point(new Position(2d, 2d))]

    @CompileStatic
    @SuppressWarnings('UnusedVariable')
    def 'constructor should accept lists containing subtype of Geometry'() {
        expect:
        GeometryCollection gc = new GeometryCollection((List<Point>) geometries)
    }

    def 'constructor should set geometries'() {
        expect:
        new GeometryCollection(geometries).geometries == geometries
    }

    def 'constructors should throw if preconditions are violated'() {
        when:
        new GeometryCollection(null)

        then:
        thrown(IllegalArgumentException)

        when:
        new GeometryCollection([new Point(new Position(1d, 2d)), new Position([40.0d, 19.0d]), null])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should get type'() {
        expect:
        new GeometryCollection(geometries).type == GeoJsonObjectType.GEOMETRY_COLLECTION
    }

    def 'equals, hashcode and toString should be overridden'() {
        expect:
        new GeometryCollection(geometries) == new GeometryCollection(geometries)
        new GeometryCollection(geometries).hashCode() == new GeometryCollection(geometries).hashCode()
        new GeometryCollection(geometries).toString() ==
        'GeometryCollection{geometries=[Point{coordinate=Position{values=[1.0, 2.0]}}, Point{coordinate=Position{values=[2.0, 2.0]}}]}'
    }
}
