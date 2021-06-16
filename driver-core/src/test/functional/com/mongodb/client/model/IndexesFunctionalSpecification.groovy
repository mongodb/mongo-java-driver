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
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.serverVersionGreaterThan
import static com.mongodb.client.model.Indexes.ascending
import static com.mongodb.client.model.Indexes.compoundIndex
import static com.mongodb.client.model.Indexes.descending
import static com.mongodb.client.model.Indexes.geo2d
import static com.mongodb.client.model.Indexes.geo2dsphere
import static com.mongodb.client.model.Indexes.geoHaystack
import static com.mongodb.client.model.Indexes.hashed
import static com.mongodb.client.model.Indexes.text
import static org.bson.BsonDocument.parse

class IndexesFunctionalSpecification extends OperationFunctionalSpecification {

    def 'ascending'() {
        when:
        getCollectionHelper().createIndex(ascending('x'))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{x : 1}'))

        when:
        getCollectionHelper().createIndex(ascending('x', 'y'))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{x : 1, y: 1}'))

        when:
        getCollectionHelper().createIndex(ascending(['a', 'b']))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{a : 1, b: 1}'))
    }

    def 'descending'() {
        when:
        getCollectionHelper().createIndex(descending('x'))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{x : -1}'))

        when:
        getCollectionHelper().createIndex(descending('x', 'y'))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{x : -1, y: -1}'))

        when:
        getCollectionHelper().createIndex(descending(['a', 'b']))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{a : -1, b: -1}'))
    }

    def 'geo2dsphere'() {
        when:
        getCollectionHelper().createIndex(geo2dsphere('x'))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{x : "2dsphere"}'))

        when:
        getCollectionHelper().createIndex(geo2dsphere('x', 'y'))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{x : "2dsphere", y: "2dsphere"}'))

        when:
        getCollectionHelper().createIndex(geo2dsphere(['a', 'b']))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{a : "2dsphere", b: "2dsphere"}'))
    }

    def 'geo2d'() {
        when:
        getCollectionHelper().createIndex(geo2d('x'))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{x : "2d"}'))
    }

    @IgnoreIf({ serverVersionGreaterThan('4.4') })
    def 'geoHaystack'() {
        when:
        getCollectionHelper().createIndex(geoHaystack('x', descending('b')), 2.0)

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{x : "geoHaystack", b: -1}'))
    }

    def 'text helper'() {
        when:
        getCollectionHelper().createIndex(text('x'))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{_fts: "text", _ftsx: 1}'))
    }

    def 'text wildcard'() {
        when:
        getCollectionHelper().createIndex(text())

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{_fts: "text", _ftsx: 1}'))
    }

    def 'hashed'() {
        when:
        getCollectionHelper().createIndex(hashed('x'))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{x : "hashed"}'))
    }

    def 'compoundIndex'() {
        when:
        getCollectionHelper().createIndex(compoundIndex(ascending('a'), descending('b')))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{a : 1, b : -1}'))

        when:
        getCollectionHelper().createIndex(compoundIndex([ascending('x'), descending('y')]))

        then:
        getCollectionHelper().listIndexes()*.get('key').contains(parse('{x : 1, y : -1}'))
    }
}
