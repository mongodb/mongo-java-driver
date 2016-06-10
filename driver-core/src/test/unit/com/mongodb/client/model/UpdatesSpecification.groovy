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

package com.mongodb.client.model

import org.bson.BsonDocument
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.conversions.Bson
import spock.lang.Specification

import static com.mongodb.client.model.Updates.addEachToSet
import static com.mongodb.client.model.Updates.addToSet
import static com.mongodb.client.model.Updates.bitwiseAnd
import static com.mongodb.client.model.Updates.bitwiseOr
import static com.mongodb.client.model.Updates.bitwiseXor
import static com.mongodb.client.model.Updates.combine
import static com.mongodb.client.model.Updates.currentDate
import static com.mongodb.client.model.Updates.currentTimestamp
import static com.mongodb.client.model.Updates.inc
import static com.mongodb.client.model.Updates.max
import static com.mongodb.client.model.Updates.min
import static com.mongodb.client.model.Updates.mul
import static com.mongodb.client.model.Updates.popFirst
import static com.mongodb.client.model.Updates.popLast
import static com.mongodb.client.model.Updates.pull
import static com.mongodb.client.model.Updates.pullAll
import static com.mongodb.client.model.Updates.pullByFilter
import static com.mongodb.client.model.Updates.push
import static com.mongodb.client.model.Updates.pushEach
import static com.mongodb.client.model.Updates.rename
import static com.mongodb.client.model.Updates.set
import static com.mongodb.client.model.Updates.setOnInsert
import static com.mongodb.client.model.Updates.unset
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class UpdatesSpecification extends Specification {
    def registry = fromProviders([new BsonValueCodecProvider(), new ValueCodecProvider()])

    def 'should render $set'() {
        expect:
        toBson(set('x', 1)) == parse('{$set : { x : 1} }')
        toBson(set('x', null)) == parse('{$set : { x : null } }')
    }

    def 'should render $setOnInsert'() {
        expect:
        toBson(setOnInsert('x', 1)) == parse('{$setOnInsert : { x : 1} }')
        toBson(setOnInsert('x', null)) == parse('{$setOnInsert : { x : null } }')
    }

    def 'should render $unset'() {
        expect:
        toBson(unset('x')) == parse('{$unset : { x : ""} }')
    }

    def 'should render $rename'() {
        expect:
        toBson(rename('x', 'y')) == parse('{$rename : { x : "y"} }')
    }

    def 'should render $inc'() {
        expect:
        toBson(inc('x', 1)) == parse('{$inc : { x : 1} }')
        toBson(inc('x', 5L)) == parse('{$inc : { x : {$numberLong : "5"}} }')
        toBson(inc('x', 3.4d)) == parse('{$inc : { x : 3.4} }')
    }

    def 'should render $mul'() {
        expect:
        toBson(mul('x', 1)) == parse('{$mul : { x : 1} }')
        toBson(mul('x', 5L)) == parse('{$mul : { x : {$numberLong : "5"}} }')
        toBson(mul('x', 3.4d)) == parse('{$mul : { x : 3.4} }')
    }

    def 'should render $min'() {
        expect:
        toBson(min('x', 42)) == parse('{$min : { x : 42} }')
    }

    def 'should render $max'() {
        expect:
        toBson(max('x', 42)) == parse('{$max : { x : 42} }')
    }

    def 'should render $currentDate'() {
        expect:
        toBson(currentDate('x')) == parse('{$currentDate : { x : true} }')
        toBson(currentTimestamp('x')) == parse('{$currentDate : { x : {$type : "timestamp"} } }')
    }

    def 'should render $addToSet'() {
        expect:
        toBson(addToSet('x', 1)) == parse('{$addToSet : { x : 1} }')
        toBson(addEachToSet('x', [1, 2, 3])) == parse('{$addToSet : { x : { $each : [1, 2, 3] } } }')
    }

    def 'should render $push'() {
        expect:
        toBson(push('x', 1)) == parse('{$push : { x : 1} }')
        toBson(pushEach('x', [1, 2, 3], new PushOptions())) == parse('{$push : { x : { $each : [1, 2, 3] } } }')
        toBson(pushEach('x', [parse('{score : 89}'), parse('{score : 65}')],
                        new PushOptions().position(0).slice(3).sortDocument(parse('{score : -1}')))) ==
        parse('{$push : { x : { $each : [{score : 89}, {score : 65}], $position : 0, $slice : 3, $sort : { score : -1 } } } }')

        toBson(pushEach('x', [89, 65],
                        new PushOptions().position(0).slice(3).sort(-1))) ==
        parse('{$push : { x : { $each : [89, 65], $position : 0, $slice : 3, $sort : -1 } } }')
    }

    def 'should render "$pull'() {
        expect:
        toBson(pull('x', 1)) == parse('{$pull : { x : 1} }')
        toBson(pullByFilter(Filters.gte('x', 5))) == parse('{$pull : { x : { $gte : 5 }} }')
    }

    def 'should render "$pullAll'() {
        expect:
        toBson(pullAll('x', [])) == parse('{$pullAll : { x : []} }')
        toBson(pullAll('x', [1, 2, 3])) == parse('{$pullAll : { x : [1, 2, 3]} }')
    }

    def 'should render $pop'() {
        expect:
        toBson(popFirst('x')) == parse('{$pop : { x : -1} }')
        toBson(popLast('x')) == parse('{$pop : { x : 1} }')
    }


    def 'should render $bit'() {
        expect:
        toBson(bitwiseAnd('x', 5)) == parse('{$bit : { x : {and : 5} } }')
        toBson(bitwiseAnd('x', 5L)) == parse('{$bit : { x : {and : {$numberLong : "5"} } } }')
        toBson(bitwiseOr('x', 5)) == parse('{$bit : { x : {or : 5} } }')
        toBson(bitwiseOr('x', 5L)) == parse('{$bit : { x : {or : {$numberLong : "5"} } } }')
        toBson(bitwiseXor('x', 5)) == parse('{$bit : { x : {xor : 5} } }')
        toBson(bitwiseXor('x', 5L)) == parse('{$bit : { x : {xor : {$numberLong : "5"} } } }')
    }


    def 'should combine updates'() {
        expect:
        toBson(combine(set('x', 1))) == parse('{$set : { x : 1} }')
        toBson(combine(set('x', 1), set('y', 2))) == parse('{$set : { x : 1, y : 2} }')
        toBson(combine(set('x', 1), set('x', 2))) == parse('{$set : { x : 2} }')
        toBson(combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4))) == parse('''{
                                                                                          $set : { x : 1, y : 2},
                                                                                          $inc : { z : 3, a : 4}}
                                                                                        }''')

        toBson(combine(combine(set('x', 1)))) == parse('{$set : { x : 1} }')
        toBson(combine(combine(set('x', 1), set('y', 2)))) == parse('{$set : { x : 1, y : 2} }')
        toBson(combine(combine(set('x', 1), set('x', 2)))) == parse('{$set : { x : 2} }')

        toBson(combine(combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4)))) == parse('''{
                                                                                                   $set : { x : 1, y : 2},
                                                                                                   $inc : { z : 3, a : 4}
                                                                                                  }''')
    }

    def 'should create string representation for simple updates'() {
        expect:
        set('x', 1).toString() == 'Update{fieldName=\'x\', operator=\'$set\', value=1}'
    }

    def 'should create string representation for with each update'() {
        expect:
        addEachToSet('x', [1, 2, 3]).toString() == 'Each Update{fieldName=\'x\', operator=\'$addToSet\', values=[1, 2, 3]}'
    }
    def 'should create string representation for push each update'() {
        expect:
        pushEach('x', [89, 65], new PushOptions().position(0).slice(3).sort(-1)).toString() ==
                'Each Update{fieldName=\'x\', operator=\'$push\', values=[89, 65], ' +
                'options=Push Options{position=0, slice=3, sort=-1}}'
        pushEach('x', [89, 65], new PushOptions().position(0).slice(3).sortDocument(parse('{x : 1}'))).toString() ==
                'Each Update{fieldName=\'x\', operator=\'$push\', values=[89, 65], ' +
                'options=Push Options{position=0, slice=3, sortDocument={ "x" : 1 }}}'
    }

    def 'should create string representation for pull all update'() {
        expect:
        pullAll('x', [1, 2, 3]).toString() == 'Update{fieldName=\'x\', operator=\'$pullAll\', value=[1, 2, 3]}'
    }

    def 'should create string representation for combined update'() {
        expect:
        combine(set('x', 1), inc('z', 3)).toString() ==
                'Updates{updates=[' +
                'Update{fieldName=\'x\', operator=\'$set\', value=1}, ' +
                'Update{fieldName=\'z\', operator=\'$inc\', value=3}]}'
    }

    def toBson(Bson bson) {
        bson.toBsonDocument(BsonDocument, registry)
    }

}
