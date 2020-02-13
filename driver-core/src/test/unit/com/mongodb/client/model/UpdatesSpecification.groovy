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

import spock.lang.Specification

import static com.mongodb.client.model.BsonHelper.toBson
import static com.mongodb.client.model.Updates.addEachToSet
import static com.mongodb.client.model.Updates.addToSet
import static com.mongodb.client.model.Updates.bitwiseAnd
import static com.mongodb.client.model.Updates.bitwiseOr
import static com.mongodb.client.model.Updates.bitwiseXor
import static com.mongodb.client.model.Updates.combine
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

@SuppressWarnings('deprecation')
class UpdatesSpecification extends Specification {

    def 'should render $set'() {
        expect:
        toBson(set('x', 1)) == parse('{$set : { x : 1} }')
        toBson(set('x', null)) == parse('{$set : { x : null } }')
    }

    def 'should render $setOnInsert'() {
        expect:
        toBson(setOnInsert('x', 1)) == parse('{$setOnInsert : { x : 1} }')
        toBson(setOnInsert('x', null)) == parse('{$setOnInsert : { x : null } }')
        toBson(setOnInsert(parse('{ a : 1, b: "two"}'))) == parse('{$setOnInsert : {a: 1, b: "two"} }')

        when:
        toBson(setOnInsert(null))

        then:
        thrown IllegalArgumentException
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
        toBson(Updates.currentDate('x')) == parse('{$currentDate : { x : true} }')
        toBson(Updates.currentTimestamp('x')) == parse('{$currentDate : { x : {$type : "timestamp"} } }')
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
                'options=Push Options{position=0, slice=3, sortDocument={"x": 1}}}'
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

    def 'should test equals for SimpleBsonKeyValue'() {
        expect:
        setOnInsert('x', 1).equals(setOnInsert('x', 1))
        setOnInsert('x', null).equals(setOnInsert('x', null))
        setOnInsert(parse('{ a : 1, b: "two"}')).equals(setOnInsert(parse('{ a : 1, b: "two"}')))
    }

    def 'should test hashCode for SimpleBsonKeyValue'() {
        expect:
        setOnInsert('x', 1).hashCode() == setOnInsert('x', 1).hashCode()
        setOnInsert('x', null).hashCode() == setOnInsert('x', null).hashCode()
        setOnInsert(parse('{ a : 1, b: "two"}')).hashCode() == setOnInsert(parse('{ a : 1, b: "two"}')).hashCode()
    }

    def 'should test equals for SimpleUpdate'() {
        expect:
        setOnInsert('x', 1).equals(setOnInsert('x', 1))
        setOnInsert('x', null).equals(setOnInsert('x', null))
        setOnInsert(parse('{ a : 1, b: "two"}')).equals(setOnInsert(parse('{ a : 1, b: "two"}')))
        rename('x', 'y').equals(rename('x', 'y'))
        inc('x', 1).equals(inc('x', 1))
        inc('x', 5L).equals(inc('x', 5L))
        inc('x', 3.4d).equals(inc('x', 3.4d))
        mul('x', 1).equals(mul('x', 1))
        mul('x', 5L).equals(mul('x', 5L))
        mul('x', 3.4d).equals(mul('x', 3.4d))
        min('x', 42).equals(min('x', 42))
        max('x', 42).equals(max('x', 42))
        Updates.currentDate('x').equals(Updates.currentDate('x'))
        Updates.currentTimestamp('x').equals(Updates.currentTimestamp('x'))
        addToSet('x', 1).equals(addToSet('x', 1))
        addEachToSet('x', [1, 2, 3]).equals(addEachToSet('x', [1, 2, 3]))
        push('x', 1).equals(push('x', 1))
        pull('x', 1).equals(pull('x', 1))
        popFirst('x').equals(popFirst('x'))
        popLast('x').equals(popLast('x'))
    }

    def 'should test hashCode for SimpleUpdate'() {
        expect:
        setOnInsert('x', 1).hashCode() == setOnInsert('x', 1).hashCode()
        setOnInsert('x', null).hashCode() == setOnInsert('x', null).hashCode()
        setOnInsert(parse('{ a : 1, b: "two"}')).hashCode() == setOnInsert(parse('{ a : 1, b: "two"}')).hashCode()
        rename('x', 'y').hashCode() == rename('x', 'y').hashCode()
        inc('x', 1).hashCode() == inc('x', 1).hashCode()
        inc('x', 5L).hashCode() == inc('x', 5L).hashCode()
        inc('x', 3.4d).hashCode() == inc('x', 3.4d).hashCode()
        mul('x', 1).hashCode() == mul('x', 1).hashCode()
        mul('x', 5L).hashCode() == mul('x', 5L).hashCode()
        mul('x', 3.4d).hashCode() == mul('x', 3.4d).hashCode()
        min('x', 42).hashCode() == min('x', 42).hashCode()
        max('x', 42).hashCode() == max('x', 42).hashCode()
        Updates.currentDate('x').hashCode() == Updates.currentDate('x').hashCode()
        Updates.currentTimestamp('x').hashCode() == Updates.currentTimestamp('x').hashCode()
        addToSet('x', 1).hashCode() == addToSet('x', 1).hashCode()
        addEachToSet('x', [1, 2, 3]).hashCode() == addEachToSet('x', [1, 2, 3]).hashCode()
        push('x', 1).hashCode() == push('x', 1).hashCode()
        pull('x', 1).hashCode() == pull('x', 1).hashCode()
        popFirst('x').hashCode() == popFirst('x').hashCode()
        popLast('x').hashCode() == popLast('x').hashCode()
    }

    def 'should test equals for WithEachUpdate'() {
        expect:
        addEachToSet('x', [1, 2, 3]).equals(addEachToSet('x', [1, 2, 3]))
    }

    def 'should test hashCode for WithEachUpdate'() {
        expect:
        addEachToSet('x', [1, 2, 3]).hashCode() == addEachToSet('x', [1, 2, 3]).hashCode()
    }

    def 'should test equals for PushUpdate'() {
        expect:
        pushEach('x', [1, 2, 3], new PushOptions()).equals(pushEach('x', [1, 2, 3], new PushOptions()))
        pushEach('x', [parse('{score : 89}'), parse('{score : 65}')],
                new PushOptions().position(0).slice(3).sortDocument(parse('{score : -1}')))
        .equals(pushEach('x', [parse('{score : 89}'), parse('{score : 65}')],
                new PushOptions().position(0).slice(3).sortDocument(parse('{score : -1}'))))

        pushEach('x', [89, 65],
                new PushOptions().position(0).slice(3).sort(-1))
        .equals(pushEach('x', [89, 65],
                new PushOptions().position(0).slice(3).sort(-1)))
    }

    def 'should test hashCode for PushUpdate'() {
        expect:
        pushEach('x', [1, 2, 3], new PushOptions()).hashCode() == pushEach('x', [1, 2, 3], new PushOptions()).hashCode()
        pushEach('x', [parse('{score : 89}'), parse('{score : 65}')],
                new PushOptions().position(0).slice(3).sortDocument(parse('{score : -1}'))).hashCode() ==
                pushEach('x', [parse('{score : 89}'), parse('{score : 65}')],
                new PushOptions().position(0).slice(3).sortDocument(parse('{score : -1}'))).hashCode()

        pushEach('x', [89, 65],
                new PushOptions().position(0).slice(3).sort(-1)).hashCode() ==
                pushEach('x', [89, 65], new PushOptions().position(0).slice(3).sort(-1)).hashCode()
    }

    def 'should test equals for PullAllUpdate'() {
        expect:
        pullAll('x', []).equals(pullAll('x', []))
        pullAll('x', [1, 2, 3]).equals(pullAll('x', [1, 2, 3]))
    }

    def 'should test hashCode for PullAllUpdate'() {
        expect:
        pullAll('x', []).hashCode() == pullAll('x', []).hashCode()
        pullAll('x', [1, 2, 3]).hashCode() == pullAll('x', [1, 2, 3]).hashCode()
    }

    def 'should test equals for CompositeUpdate'() {
        expect:
        combine(set('x', 1)).equals(combine(set('x', 1)))
        combine(set('x', 1), set('y', 2)).equals(combine(set('x', 1), set('y', 2)))
        combine(set('x', 1), set('x', 2)).equals(combine(set('x', 1), set('x', 2)))
        combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4))
                .equals(combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4)))

        combine(combine(set('x', 1))).equals(combine(combine(set('x', 1))))
        combine(combine(set('x', 1), set('y', 2))).equals(combine(combine(set('x', 1), set('y', 2))))
        combine(combine(set('x', 1), set('x', 2))).equals(combine(combine(set('x', 1), set('x', 2))))

        combine(combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4)))
                .equals(combine(combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4))))
    }

    def 'should test hashCode for CompositeUpdate'() {
        expect:
        combine(set('x', 1)).hashCode() == combine(set('x', 1)).hashCode()
        combine(set('x', 1), set('y', 2)).hashCode() == combine(set('x', 1), set('y', 2)).hashCode()
        combine(set('x', 1), set('x', 2)).equals(combine(set('x', 1), set('x', 2)))
        combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4)).hashCode() ==
                combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4)).hashCode()

        combine(combine(set('x', 1))).hashCode() == combine(combine(set('x', 1))).hashCode()
        combine(combine(set('x', 1), set('y', 2))).hashCode() == combine(combine(set('x', 1), set('y', 2))).hashCode()
        combine(combine(set('x', 1), set('x', 2))).hashCode() == combine(combine(set('x', 1), set('x', 2))).hashCode()

        combine(combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4))).hashCode() ==
                combine(combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4))).hashCode()
    }
}
