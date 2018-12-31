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

import static com.mongodb.client.model.Sorts.ascending
import static com.mongodb.client.model.Sorts.descending
import static com.mongodb.client.model.Sorts.metaTextScore
import static com.mongodb.client.model.Sorts.orderBy
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class SortsSpecification extends Specification {
    def registry = fromProviders([new BsonValueCodecProvider(), new ValueCodecProvider()])

    def 'ascending'() {
        expect:
        toBson(ascending('x')) == parse('{x : 1}')
        toBson(ascending('x', 'y')) == parse('{x : 1, y : 1}')
        toBson(ascending(['x', 'y'])) == parse('{x : 1, y : 1}')
    }

    def 'descending'() {
        expect:
        toBson(descending('x')) == parse('{x : -1}')
        toBson(descending('x', 'y')) == parse('{x : -1, y : -1}')
        toBson(descending(['x', 'y'])) == parse('{x : -1, y : -1}')
    }

    def 'metaTextScore'() {
        expect:
        toBson(metaTextScore('x')) == parse('{x : {$meta : "textScore"}}')
    }

    def 'orderBy'() {
        expect:
        toBson(orderBy([ascending('x'), descending('y')])) == parse('{x : 1, y : -1}')
        toBson(orderBy(ascending('x'), descending('y'))) == parse('{x : 1, y : -1}')
        toBson(orderBy(ascending('x'), descending('y'), descending('x'))) == parse('{y : -1, x : -1}')
        toBson(orderBy(ascending('x', 'y'), descending('a', 'b'))) == parse('{x : 1, y : 1, a : -1, b : -1}')
    }

    def 'should create string representation for simple sorts'() {
        expect:
        ascending('x', 'y').toString() == '{"x": 1, "y": 1}'
        descending('x', 'y').toString() == '{"x": -1, "y": -1}'
        metaTextScore('x').toString() == '{"x": {"$meta": "textScore"}}'
    }

    def 'should create string representation for compound sorts'() {
        expect:
        orderBy(ascending('x', 'y'), descending('a', 'b')).toString() ==
                'Compound Sort{sorts=[{"x": 1, "y": 1}, {"a": -1, "b": -1}]}'
    }

    def toBson(Bson bson) {
        bson.toBsonDocument(BsonDocument, registry)
    }
}
