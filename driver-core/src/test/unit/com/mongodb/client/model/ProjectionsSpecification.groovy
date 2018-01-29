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

import org.bson.BsonDocument
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.conversions.Bson
import spock.lang.Specification

import static com.mongodb.client.model.Filters.and
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Projections.computed
import static com.mongodb.client.model.Projections.elemMatch
import static com.mongodb.client.model.Projections.exclude
import static com.mongodb.client.model.Projections.excludeId
import static com.mongodb.client.model.Projections.fields
import static com.mongodb.client.model.Projections.include
import static com.mongodb.client.model.Projections.metaTextScore
import static com.mongodb.client.model.Projections.slice
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class ProjectionsSpecification extends Specification {
    def registry = fromProviders([new BsonValueCodecProvider(), new ValueCodecProvider()])

    def 'include'() {
        expect:
        toBson(include('x')) == parse('{x : 1}')
        toBson(include('x', 'y')) == parse('{x : 1, y : 1}')
        toBson(include(['x', 'y'])) == parse('{x : 1, y : 1}')
        toBson(include(['x', 'y', 'x'])) == parse('{y : 1, x : 1}')
    }

    def 'exclude'() {
        expect:
        toBson(exclude('x')) == parse('{x : 0}')
        toBson(exclude('x', 'y')) == parse('{x : 0, y : 0}')
        toBson(exclude(['x', 'y'])) == parse('{x : 0, y : 0}')
    }

    def 'excludeId'() {
        expect:
        toBson(excludeId()) == parse('{_id : 0}')
    }

    def 'firstElem'() {
        expect:
        toBson(elemMatch('x')) == parse('{"x.$" : 1}')
    }

    def 'elemMatch'() {
        expect:
        toBson(elemMatch('x', and(eq('y', 1), eq('z', 2)))) == parse('{x : {$elemMatch : {y : 1, z : 2}}}')
    }

    def 'slice'() {
        expect:
        toBson(slice('x', 5)) == parse('{x : {$slice : 5}}')
        toBson(slice('x', 5, 10)) == parse('{x : {$slice : [5, 10]}}')
    }

    def 'metaTextScore'() {
        expect:
        toBson(metaTextScore('x')) == parse('{x : {$meta : "textScore"}}')
    }

    def 'computed'() {
        expect:
        toBson(computed('c', '$y')) == parse('{c : "$y"}')
    }

    def 'combine fields'() {
        expect:
        toBson(fields(include('x', 'y'), exclude('_id'))) == parse('{x : 1, y : 1, _id : 0}')
        toBson(fields([include('x', 'y'), exclude('_id')])) == parse('{x : 1, y : 1, _id : 0}')
        toBson(fields(include('x', 'y'), exclude('x'))) == parse('{y : 1, x : 0}')
    }

    def 'should create string representation for include and exclude'() {
        expect:
        include(['x', 'y', 'x']).toString() == '{ "y" : 1, "x" : 1 }'
        exclude(['x', 'y', 'x']).toString() == '{ "y" : 0, "x" : 0 }'
        excludeId().toString() == '{ "_id" : 0 }'
    }

    def 'should create string representation for computed'() {
        expect:
        computed('c', '$y').toString() == 'Expression{name=\'c\', expression=$y}'
    }

    def 'should create string representation for elemMatch with filter'() {
        expect:
        elemMatch('x', and(eq('y', 1), eq('z', 2))).toString() ==
                'ElemMatch Projection{fieldName=\'x\', ' +
                'filter=And Filter{filters=[Filter{fieldName=\'y\', value=1}, Filter{fieldName=\'z\', value=2}]}}'
    }

    def 'should create string representation for fields'() {
        expect:
        fields(include('x', 'y'), exclude('_id')).toString() == 'Projections{projections=[{ "x" : 1, "y" : 1 }, { "_id" : 0 }]}'
    }

    def toBson(Bson bson) {
        bson.toBsonDocument(BsonDocument, registry)
    }
}
