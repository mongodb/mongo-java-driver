package com.mongodb.client.model

import spock.lang.Specification


class InsertManyOptionsSpecification extends Specification {
    def 'should default to ordered'() {
        expect:
        new InsertManyOptions().ordered
    }

    def 'should set ordered'() {
        expect:
        new InsertManyOptions().ordered(ordered).ordered == ordered

        where:
        ordered << [true, false]
    }
}