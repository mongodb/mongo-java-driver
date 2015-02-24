package com.mongodb.client.model

import org.bson.BsonDocument
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.client.model.FilterBuilder.and
import static com.mongodb.client.model.FilterBuilder.eq
import static com.mongodb.client.model.FilterBuilder.exists
import static com.mongodb.client.model.FilterBuilder.gt
import static com.mongodb.client.model.FilterBuilder.gte
import static com.mongodb.client.model.FilterBuilder.lt
import static com.mongodb.client.model.FilterBuilder.lte
import static com.mongodb.client.model.FilterBuilder.or
import static org.bson.BsonDocument.parse

class FilterBuilderSpecification extends Specification {
    def codecRegistry = new RootCodecRegistry([new BsonValueCodecProvider(), new ValueCodecProvider()]);

    def 'should render filter for eq'() {
       expect:
       eq('x', 1).render(BsonDocument, codecRegistry) == parse('{x : 1}')
    }

    def 'should render filter for $gt'() {
        expect:
        gt('x', 1).render(BsonDocument, codecRegistry) == parse('{x : {$gt : 1} }')
    }

    def 'should render filter for $lt'() {
        expect:
        lt('x', 1).render(BsonDocument, codecRegistry) == parse('{x : {$lt : 1} }')
    }

    def 'should render filter for $gte'() {
        expect:
        gte('x', 1).render(BsonDocument, codecRegistry) == parse('{x : {$gte : 1} }')
    }

    def 'should render filter for $lte'() {
        expect:
        lte('x', 1).render(BsonDocument, codecRegistry) == parse('{x : {$lte : 1} }')
    }

    def 'should render filter for $exists'() {
        expect:
        exists('x').render(BsonDocument, codecRegistry) == parse('{x : {$exists : true} }')

        exists('x', false).render(BsonDocument, codecRegistry) == parse('{x : {$exists : false} }')
    }

    def 'should render filter for or'() {
        expect:
        or([eq('x', 1), eq('y', 2)]).render(BsonDocument, codecRegistry) == parse('{$or : [{x : 1}, {y : 2}]}')
        or(eq('x', 1), eq('y', 2)).render(BsonDocument, codecRegistry) == parse('{$or : [{x : 1}, {y : 2}]}')
    }

    def 'should render filter for and'() {
        expect:
        and([eq('x', 1), eq('y', 2)]).render(BsonDocument, codecRegistry) == parse('{x : 1, y : 2}')
        and(eq('x', 1), eq('y', 2)).render(BsonDocument, codecRegistry) == parse('{x : 1, y : 2}')
    }

    def 'should render and with clashing keys by promoting to dollar form'() {
        expect:
        and([eq('a', 1), eq('a', 2)]).render(BsonDocument, codecRegistry) == parse('{$and: [{a: 1}, {a: 2}]}');
    }

    def 'should flatten multiple operators for the same key'() {
        expect:
        and([gt('a', 1), lt('a', 9)]).render(BsonDocument, codecRegistry) == parse('{a : {$gt : 1, $lt : 9}}');
    }

    def 'should flatten nested and filter'() {
        expect:
        and([and([eq('a', 1), eq('b', 2)]), eq('c', 3)]).render(BsonDocument, codecRegistry) == parse('{a : 1, b : 2, c : 3}')
        and([and([eq('a', 1), eq('a', 2)]), eq('c', 3)]).render(BsonDocument, codecRegistry) == parse('{$and:[{a : 1}, {a : 2}, {c : 3}] }')
        and([lt('a', 1), lt('b', 2)]).render(BsonDocument, codecRegistry) == parse('{a : {$lt : 1}, b : {$lt : 2} }')
        and([lt('a', 1), lt('a', 2)]).render(BsonDocument, codecRegistry) == parse('{$and : [{a : {$lt : 1}}, {a : {$lt : 2}}]}')
    }

}