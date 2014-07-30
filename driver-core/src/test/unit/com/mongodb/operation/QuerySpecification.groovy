/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.operation

import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.operation.QueryFlag.AwaitData
import static com.mongodb.operation.QueryFlag.Exhaust
import static com.mongodb.operation.QueryFlag.SlaveOk
import static com.mongodb.operation.QueryFlag.Tailable

class QuerySpecification extends Specification {
    def 'should return #numberToReturn as numberToReturn for #query'() {
        expect:
        query.getNumberToReturn() == numberToReturn

        where:
        query                               | numberToReturn
        new Find()                          | 0
        new Find().limit(-10)               | -10
        new Find().batchSize(10)            | 10
        new Find().limit(10)                | 10
        new Find().limit(10).batchSize(15)  | 10
        new Find().limit(10).batchSize(-15) | 10
        new Find().limit(10).batchSize(7)   | 7
        new Find().limit(10).batchSize(-7)  | -7
    }

    def 'test flags with primary'() {
        expect:
        query.getFlags(primary()) == flags

        where:
        query                                                                         | flags
        new Find()                                                                    | EnumSet.noneOf(QueryFlag)
        new Find().addFlags(EnumSet.of(Tailable))                                     | EnumSet.of(Tailable, AwaitData)
        new Find().addFlags(EnumSet.of(SlaveOk))                                      | EnumSet.of(SlaveOk)
        new Find().addFlags(EnumSet.of(Tailable, SlaveOk))                            | EnumSet.of(Tailable, AwaitData, SlaveOk)
        new Find().flags(EnumSet.of(Exhaust))                                         | EnumSet.of(Exhaust)
        new Find().addFlags(EnumSet.of(Tailable, SlaveOk)).flags(EnumSet.of(Exhaust)) | EnumSet.of(Exhaust)
    }

    def 'test flags with secondary'() {
        expect:
        query.getFlags(secondary()) == flags

        where:
        query                                    | flags
        new Find()                               | EnumSet.of(SlaveOk)
        new Find().addFlags(EnumSet.of(SlaveOk)) | EnumSet.of(SlaveOk)
        new Find().flags(EnumSet.of(Exhaust))    | EnumSet.of(Exhaust, SlaveOk)
    }

    def 'should throw an exception if options given are null'() {
        when:
        new Find().options(null);
        then:
        thrown(IllegalArgumentException)

        when:
        new Find().addFlags(null);
        then:
        thrown(IllegalArgumentException)
    }

    def 'testCopyConstructor'() {
        given:
        EnumSet<QueryFlag> flags = EnumSet.allOf(QueryFlag)
        BsonDocument hint = new BsonDocument('a', new BsonInt32(1))
        int batchSize = 2
        int limit = 5
        int skip = 1
        Find originalQuery = new Find();
        originalQuery
                .addFlags(flags)
                .hintIndex(hint)
                .batchSize(batchSize)
                .limit(limit)
                .skip(skip);

        when:
        Query copy = new Find(originalQuery);

        then:
        copy.getFlags(primary()) == flags
        copy.getBatchSize() == batchSize
        copy.getLimit() == limit
        copy.getSkip() == skip
        copy.getHint() == hint
    }

    def 'should support index hints'() {
        expect:
        query.getHint() == value

        where:
        query                                                         | value
        new Find()                                                    | null
        new Find().hintIndex('i_1')                                   | new BsonString('i_1')
        new Find().hintIndex(new BsonDocument('i', new BsonInt32(1))) | new BsonDocument('i', new BsonInt32(1))
    }
}
