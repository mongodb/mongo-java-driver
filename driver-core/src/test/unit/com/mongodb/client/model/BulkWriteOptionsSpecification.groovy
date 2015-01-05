package com.mongodb.client.model

import spock.lang.Specification


class BulkWriteOptionsSpecification extends Specification {
    def 'should default to ordered'() {
        expect:
        new BulkWriteOptions().ordered
    }

    def 'should set ordered'() {
        expect:
        new BulkWriteOptions().ordered(ordered).ordered == ordered

        where:
        ordered << [true, false]
    }
}