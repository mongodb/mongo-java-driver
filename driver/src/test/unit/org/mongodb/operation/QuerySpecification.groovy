package org.mongodb.operation

import org.mongodb.Document
import org.mongodb.ReadPreference
import spock.lang.Specification

import static org.mongodb.operation.QueryFlag.Exhaust
import static org.mongodb.operation.QueryFlag.SlaveOk
import static org.mongodb.operation.QueryFlag.Tailable

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

    def 'testOptions'() {
        expect:
        query.getFlags() == flags

        where:
        query                                                                           | flags
        new Find()                                                                      | EnumSet.noneOf(QueryFlag)
        new Find().addFlags(EnumSet.of(Tailable))                                       | EnumSet.of(Tailable)
        new Find().addFlags(EnumSet.of(SlaveOk))                                        | EnumSet.of(SlaveOk)
        new Find().addFlags(EnumSet.of(Tailable, SlaveOk))                              | EnumSet.of(Tailable, SlaveOk)
        new Find().flags(EnumSet.of(Exhaust))                                           | EnumSet.of(Exhaust)
        new Find().addFlags(EnumSet.of(Tailable, SlaveOk)).flags(EnumSet.of(Exhaust))   | EnumSet.of(Exhaust)
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
        ReadPreference readPreference = ReadPreference.primary()
        Document hint = new Document('a', 1)
        int batchSize = 2
        int limit = 5
        int skip = 1
        Find originalQuery = new Find();
        originalQuery
                .addFlags(flags)
                .hintIndex(hint)
                .readPreference(readPreference)
                .batchSize(batchSize)
                .limit(limit)
                .skip(skip);

        when:
        Query copy = new Find(originalQuery);

        then:
        copy.getFlags() == flags
        copy.getReadPreference() == readPreference
        copy.getBatchSize() == batchSize
        copy.getLimit() == limit
        copy.getSkip() == skip
        copy.getHint().getValue() == hint
    }

    def 'should support index hints'() {
        expect:
        query.getHint()?.getValue() == value

        where:
        query                                      | value
        new Find()                                 | null
        new Find().hintIndex('i_1')                | 'i_1'
        new Find().hintIndex(new Document('i', 1)) | new Document('i', 1)
    }
}
