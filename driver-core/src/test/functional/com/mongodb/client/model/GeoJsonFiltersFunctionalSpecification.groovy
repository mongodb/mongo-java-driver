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

package com.mongodb.client.model

import com.mongodb.OperationFunctionalSpecification
import com.mongodb.client.model.geojson.Point
import com.mongodb.client.model.geojson.Polygon
import com.mongodb.client.model.geojson.Position
import org.bson.Document
import org.bson.conversions.Bson

import static com.mongodb.client.model.Filters.geoIntersects
import static com.mongodb.client.model.Filters.geoWithin
import static com.mongodb.client.model.Filters.near
import static com.mongodb.client.model.Filters.nearSphere
import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.CRS_84
import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326

class GeoJsonFiltersFunctionalSpecification extends OperationFunctionalSpecification {
    def firstPoint = new Document('_id', 1).append('geo', Document.parse(new Point(CRS_84, new Position(1d, 1d)).toJson()))
    def secondPoint = new Document('_id', 2).append('geo', Document.parse(new Point(EPSG_4326, new Position(2d, 2d)).toJson()))
    def thirdPoint = new Document('_id', 3).append('geo', Document.parse(new Point(new Position(3d, 3d)).toJson()))
    def firstPolygon = new Document('_id', 4).append('geo', Document.parse(new Polygon([new Position(2d, 2d), new Position(6d, 2d),
                                                                                        new Position(6d, 6d), new Position(2d, 6d),
                                                                                        new Position(2d, 2d)]).toJson()))

    def setup() {
        getCollectionHelper().createIndex(new Document('geo', '2dsphere'))
        getCollectionHelper().insertDocuments(firstPoint, secondPoint, thirdPoint, firstPolygon)
    }

    def 'find'(Bson filter) {
        getCollectionHelper().find(filter, new Document('_id', 1)) // sort by _id
    }

    def '$geoWithin'() {
        given:
        def polygon = new Polygon([new Position(0d, 0d), new Position(4d, 0d), new Position(4d, 4d), new Position(0d, 4d),
                                   new Position(0d, 0d)])

        expect:
        find(geoWithin('geo', polygon)) == [firstPoint, secondPoint, thirdPoint]
    }

    def '$geoIntersects'() {
        given:
        def polygon = new Polygon([new Position(0d, 0d), new Position(4d, 0d), new Position(4d, 4d), new Position(0d, 4d),
                                   new Position(0d, 0d)])

        expect:
        find(geoIntersects('geo', polygon)) == [firstPoint, secondPoint, thirdPoint, firstPolygon]
    }

    def '$near'() {
        expect:
        find(near('geo', new Point(new Position(1.01d, 1.01d)), 10000d, null)) == [firstPoint]
    }

    def '$nearSphere'() {
        expect:
        find(nearSphere('geo', new Point(new Position(1.01d, 1.01d)), 10000d, null)) == [firstPoint]
    }
}
