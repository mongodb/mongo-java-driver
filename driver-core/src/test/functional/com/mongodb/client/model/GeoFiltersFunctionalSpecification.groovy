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
import org.bson.Document
import org.bson.conversions.Bson

import static com.mongodb.client.model.Filters.geoWithinBox
import static com.mongodb.client.model.Filters.geoWithinCenter
import static com.mongodb.client.model.Filters.geoWithinCenterSphere
import static com.mongodb.client.model.Filters.geoWithinPolygon
import static com.mongodb.client.model.Filters.near
import static com.mongodb.client.model.Filters.nearSphere

class GeoFiltersFunctionalSpecification extends OperationFunctionalSpecification {
    def firstPoint = new Document('_id', 1).append('geo', [1d, 1d])
    def secondPoint = new Document('_id', 2).append('geo', [45d, 2d])
    def thirdPoint = new Document('_id', 3).append('geo', [3d, 3d])

    def setup() {
        getCollectionHelper().createIndex(new Document('geo', '2d'))
        getCollectionHelper().insertDocuments(firstPoint, secondPoint, thirdPoint)
    }

    def 'find'(Bson filter) {
        getCollectionHelper().find(filter, new Document('_id', 1)) // sort by _id
    }

    def '$near'() {
        expect:
        find(near('geo', 1.01d, 1.01d, 0.1d, 0.0d)) == [firstPoint]
    }

    def '$nearSphere'() {
        expect:
        find(nearSphere('geo', 1.01d, 1.01d, 0.1d, 0.0d)) == [firstPoint, thirdPoint]
    }

    def '$geoWithin $box'() {
        expect:
        find(geoWithinBox('geo', 0d, 0d, 4d, 4d)) == [firstPoint, thirdPoint]
    }

    def '$geoWithin $polygon'() {
        expect:
        find(geoWithinPolygon('geo', [[0d, 0d], [0d, 4d], [4d, 4d], [4d, 0d]])) == [firstPoint, thirdPoint]
    }

    def '$geoWithin $center'() {
        expect:
        find(geoWithinCenter('geo', 2d, 2d, 4d)) == [firstPoint, thirdPoint]
    }

    def '$geoWithin $centerSphere'() {
        expect:
        find(geoWithinCenterSphere('geo', 2d, 2d, 4d)) == [firstPoint, secondPoint, thirdPoint]
    }
}
