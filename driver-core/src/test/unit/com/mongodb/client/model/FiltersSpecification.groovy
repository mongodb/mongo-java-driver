/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONObjectITIONS OF ANY KINObject, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model

import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonType
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.conversions.Bson
import spock.lang.Specification

import java.util.regex.Pattern

import static Filters.and
import static Filters.eq
import static Filters.exists
import static Filters.gt
import static Filters.gte
import static Filters.lt
import static Filters.lte
import static Filters.or
import static com.mongodb.client.model.Filters.all
import static com.mongodb.client.model.Filters.elemMatch
import static com.mongodb.client.model.Filters.mod
import static com.mongodb.client.model.Filters.ne
import static com.mongodb.client.model.Filters.nin
import static com.mongodb.client.model.Filters.regex
import static com.mongodb.client.model.Filters.size
import static com.mongodb.client.model.Filters.text
import static com.mongodb.client.model.Filters.type
import static com.mongodb.client.model.Filters.where
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class FiltersSpecification extends Specification {
    def registry = fromProviders([new BsonValueCodecProvider(), new ValueCodecProvider()])

    def 'eq should render without $eq'() {
        expect:
        toBson(eq('x', 1)) == parse('{x : 1}')
        toBson(eq('x', null)) == parse('{x : null}')
    }

    def 'should render $ne'() {
        expect:
        toBson(ne('x', 1)) == parse('{x : {$ne : 1} }')
        toBson(ne('x', null)) == parse('{x : {$ne : null} }')
    }

    def 'should render $gt'() {
        expect:
        toBson(gt('x', 1)) == parse('{x : {$gt : 1} }')
    }

    def 'should render $lt'() {
        expect:
        toBson(lt('x', 1)) == parse('{x : {$lt : 1} }')
    }

    def 'should render $gte'() {
        expect:
        toBson(gte('x', 1)) == parse('{x : {$gte : 1} }')
    }

    def 'should render $lte'() {
        expect:
        toBson(lte('x', 1)) == parse('{x : {$lte : 1} }')
    }

    def 'should render $exists'() {
        expect:
        toBson(exists('x')) == parse('{x : {$exists : true} }')
        toBson(exists('x', false)) == parse('{x : {$exists : false} }')
    }

    def 'should render $or'() {
        expect:
        toBson(or([eq('x', 1), eq('y', 2)])) == parse('{$or : [{x : 1}, {y : 2}]}')
        toBson(or(eq('x', 1), eq('y', 2))) == parse('{$or : [{x : 1}, {y : 2}]}')
    }

    def 'and should render and without using $and'() {
        expect:
        toBson(and([eq('x', 1), eq('y', 2)])) == parse('{x : 1, y : 2}')
        toBson(and(eq('x', 1), eq('y', 2))) == parse('{x : 1, y : 2}')
    }

    def 'and should render $and with clashing keys'() {
        expect:
        toBson(and([eq('a', 1), eq('a', 2)])) == parse('{$and: [{a: 1}, {a: 2}]}');
    }

    def 'and should flatten multiple operators for the same key'() {
        expect:
        toBson(and([gt('a', 1), lt('a', 9)])) == parse('{a : {$gt : 1, $lt : 9}}');
    }

    def 'and should flatten nested'() {
        expect:
        toBson(and([and([eq('a', 1), eq('b', 2)]), eq('c', 3)])) == parse('{a : 1, b : 2, c : 3}')
        toBson(and([and([eq('a', 1), eq('a', 2)]), eq('c', 3)])) == parse('{$and:[{a : 1}, {a : 2}, {c : 3}] }')
        toBson(and([lt('a', 1), lt('b', 2)])) == parse('{a : {$lt : 1}, b : {$lt : 2} }')
        toBson(and([lt('a', 1), lt('a', 2)])) == parse('{$and : [{a : {$lt : 1}}, {a : {$lt : 2}}]}')
    }

    def 'should render $all'() {
        expect:
        toBson(all('a', [1, 2, 3])) == parse('{a : {$all : [1, 2, 3]} }')
        toBson(all('a', 1, 2, 3)) == parse('{a : {$all : [1, 2, 3]} }')
    }

    def 'should render $elemMatch'() {
        expect:
        toBson(elemMatch('results', new BsonDocument('$gte', new BsonInt32(80)).append('$lt', new BsonInt32(85)))) ==
        parse('{results : {$elemMatch : {$gte: 80, $lt: 85}}}')

        toBson(elemMatch('results', and(eq('product', 'xyz'), gt('score', 8)))) ==
        parse('{ results : {$elemMatch : {product : "xyz", score : {$gt : 8}}}}')
    }

    def 'should render $in'() {
        expect:
        toBson(Filters.in('a', [1, 2, 3])) == parse('{a : {$in : [1, 2, 3]} }')
        toBson(Filters.in('a', 1, 2, 3)) == parse('{a : {$in : [1, 2, 3]} }')
    }

    def 'should render $nin'() {
        expect:
        toBson(nin('a', [1, 2, 3])) == parse('{a : {$nin : [1, 2, 3]} }')
        toBson(nin('a', 1, 2, 3)) == parse('{a : {$nin : [1, 2, 3]} }')
    }

    def 'should render $mod'() {
        expect:
        toBson(mod('a', 100, 7)) == new BsonDocument('a', new BsonDocument('$mod', new BsonArray([new BsonInt64(100), new BsonInt64(7)])))
    }

    def 'should render $size'() {
        expect:
        toBson(size('a', 13)) == parse('{a : {$size : 13} }')
    }

    def 'should render $type'() {
        expect:
        toBson(type('a', BsonType.ARRAY)) == parse('{a : {$type : 4} }')
    }

    def 'should render $text'() {
        expect:
        toBson(text('I love MongoDB')) == parse('{$text : {$search : "I love MongoDB"} }')
        toBson(text('I love MongoDB', 'English')) == parse('{$text : {$search : "I love MongoDB", $language : "English"} }')
    }

    def 'should render $regex'() {
        expect:
        toBson(regex('name', 'acme.*corp')) == parse('{name : {$regex : "acme.*corp"}}')
        toBson(regex('name', 'acme.*corp', 'si')) == parse('{name : {$regex : "acme.*corp", $options : "si"}}')
        toBson(regex('name', Pattern.compile('acme.*corp'))) == parse('{name : {$regex : "acme.*corp"}}')
    }

    def 'should render $where'() {
        expect:
        toBson(where('this.credits == this.debits')) == parse('{$where: "this.credits == this.debits"}')
    }

    def toBson(Bson bson) {
        bson.toBsonDocument(BsonDocument, registry)
    }
}
