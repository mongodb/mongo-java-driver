package org.mongodb.operation

import org.mongodb.ReadPreference
import spock.lang.Specification

import static org.mongodb.operation.QueryOption.Exhaust
import static org.mongodb.operation.QueryOption.SlaveOk
import static org.mongodb.operation.QueryOption.Tailable

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
        query.getOptions() == options

        where:
        query                                                                             | options
        new Find()                                                                        | EnumSet.noneOf(QueryOption)
        new Find().addOptions(EnumSet.of(Tailable))                                       | EnumSet.of(Tailable)
        new Find().addOptions(EnumSet.of(SlaveOk))                                        | EnumSet.of(SlaveOk)
        new Find().addOptions(EnumSet.of(Tailable, SlaveOk))                              | EnumSet.of(Tailable, SlaveOk)
        new Find().options(EnumSet.of(Exhaust))                                           | EnumSet.of(Exhaust)
        new Find().addOptions(EnumSet.of(Tailable, SlaveOk)).options(EnumSet.of(Exhaust)) | EnumSet.of(Exhaust)
    }

    def 'should throw an exception if options given are null'() {
        when:
        new Find().options(null);
        then:
        thrown(IllegalArgumentException)

        when:
        new Find().addOptions(null);
        then:
        thrown(IllegalArgumentException)
    }

    def 'testCopyConstructor'() {
        given:
        EnumSet<QueryOption> options = EnumSet.allOf(QueryOption)
        ReadPreference readPreference = ReadPreference.primary()
        int batchSize = 2
        int limit = 5
        int skip = 1
        Find originalQuery = new Find();
        originalQuery.addOptions(options).readPreference(readPreference).batchSize(batchSize).limit(limit).skip(skip);

        when:
        Query copy = new Find(originalQuery);

        then:
        copy.getOptions() == options
        copy.getReadPreference() == readPreference
        copy.getBatchSize() == batchSize
        copy.getLimit() == limit
        copy.getSkip() == skip
    }

}
