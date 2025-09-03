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

package com.mongodb

import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification
import spock.lang.Unroll

import static java.util.concurrent.TimeUnit.MICROSECONDS
import static java.util.concurrent.TimeUnit.MILLISECONDS

class WriteConcernSpecification extends Specification {

    @Unroll
    def 'constructors should set up write concern #wc correctly'() {
        expect:
        wc.getWObject() == w
        wc.getWTimeout(MILLISECONDS) == wTimeout
        wc.getJournal() == journal

        where:
        wc                                                | w          | wTimeout | journal
        new WriteConcern(1)                               | 1          | null     | null
        new WriteConcern(1, 10)                           | 1          | 10       | null
        new WriteConcern((Object) null, 0, false)         | null       | 0        |  false
        new WriteConcern('majority')                      | 'majority' | null     | null
    }

    def 'test journal getters'() {
        expect:
        wc.getJournal() == journal

        where:
        wc                                        | journal
        new WriteConcern(null, null, null)  | null
        new WriteConcern(null, null, false) | false
        new WriteConcern(null, null, true)  | true
    }


    def 'test wTimeout getters'() {
        expect:
        wc.getWTimeout(MILLISECONDS) == wTimeout
        wc.getWTimeout(MICROSECONDS) == (wTimeout == null ? null : wTimeout * 1000)

        where:
        wc                                        | wTimeout
        new WriteConcern(null, null, null)  | null
        new WriteConcern(null, 1000, null) | 1000
    }

    def 'test wTimeout getter error conditions'() {
        when:
        WriteConcern.ACKNOWLEDGED.getWTimeout(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'test getWObject'() {
        expect:
        wc.getWObject() == wObject

        where:
        wc                        | wObject
        WriteConcern.ACKNOWLEDGED | null
        WriteConcern.W1           | 1
        WriteConcern.MAJORITY     | 'majority'
    }

    def 'test getWString'() {
        expect:
        wc.getWString() == wString

        where:
        wc                        | wString
        WriteConcern.MAJORITY     | 'majority'
    }

    def 'test getWString error conditions'() {
        when:
        wc.getWString()

        then:
        thrown(IllegalStateException)

        where:
        wc << [WriteConcern.ACKNOWLEDGED, WriteConcern.W1]
    }

    def 'test getW'() {
        expect:
        wc.getW() == w

        where:
        wc                          | w
        WriteConcern.UNACKNOWLEDGED | 0
        WriteConcern.W1             | 1
    }

    def 'test getW error conditions'() {
        when:
        wc.getW()

        then:
        thrown(IllegalStateException)

        where:
        wc << [WriteConcern.ACKNOWLEDGED, WriteConcern.MAJORITY]
    }

    def 'test withW methods'() {
        expect:
        WriteConcern.UNACKNOWLEDGED.withW(1) == new WriteConcern(1, null, null)
        WriteConcern.UNACKNOWLEDGED.withW('dc1') == new WriteConcern('dc1', null, null)

        when:
        WriteConcern.UNACKNOWLEDGED.withW(null)

        then:
        thrown(IllegalArgumentException)

        when:
        WriteConcern.UNACKNOWLEDGED.withW(-1)

        then:
        thrown(IllegalArgumentException)

        when:
        WriteConcern.UNACKNOWLEDGED.withJournal(true)

        then:
        thrown(IllegalArgumentException)
    }

    def 'test withJournal methods'() {
        expect:
        new WriteConcern(null, null, null).withJournal(true) == new WriteConcern(null, null, true)
    }

    def 'test withWTimeout methods'() {
        expect:
        new WriteConcern(null, null, null).withWTimeout(0, MILLISECONDS) == new WriteConcern(null, 0, null)
        new WriteConcern(null, null, null).withWTimeout(1000, MILLISECONDS) == new WriteConcern(null, 1000, null)

        when:
        WriteConcern.ACKNOWLEDGED.withWTimeout(0, null)

        then:
        thrown(IllegalArgumentException)

        when:
        WriteConcern.ACKNOWLEDGED.withWTimeout(-1, MILLISECONDS)

        then:
        thrown(IllegalArgumentException)

        when:
        WriteConcern.ACKNOWLEDGED.withWTimeout(Integer.MAX_VALUE + 1, MILLISECONDS)

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    @SuppressWarnings('DuplicateMapLiteral')
    def '#wc should return write concern document #commandDocument'() {
        expect:
        wc.asDocument() == commandDocument

        where:
        wc                                | commandDocument
        WriteConcern.UNACKNOWLEDGED       | new BsonDocument('w', new BsonInt32(0))
        WriteConcern.ACKNOWLEDGED         | new BsonDocument()
        WriteConcern.W2 | new BsonDocument('w', new BsonInt32(2))
        WriteConcern.JOURNALED            | new BsonDocument('j', BsonBoolean.TRUE)
        new WriteConcern('majority')      | new BsonDocument('w', new BsonString('majority'))
        new WriteConcern(2, 100)          | new BsonDocument('w', new BsonInt32(2)).append('wtimeout', new BsonInt32(100))
    }

    @SuppressWarnings('ExplicitCallToEqualsMethod')
    def 'test equals'() {
        expect:
        wc.equals(compareTo) == expectedResult

        where:
        wc                                   | compareTo                           | expectedResult
        WriteConcern.ACKNOWLEDGED            | WriteConcern.ACKNOWLEDGED           | true
        WriteConcern.ACKNOWLEDGED            | null                                | false
        WriteConcern.ACKNOWLEDGED            | WriteConcern.UNACKNOWLEDGED         | false
        new WriteConcern(1, 0)               | new WriteConcern(1, 1)              | false
    }

    def 'test hashCode'() {
        expect:
        wc.hashCode() == hashCode

        where:
        wc                                   | hashCode
        WriteConcern.ACKNOWLEDGED            | 0
        WriteConcern.W1                      | 961
        WriteConcern.W2                      | 1922
        WriteConcern.MAJORITY                | -322299115
    }

    def 'test constants'() {
        expect:
        constructedWriteConcern == constantWriteConcern

        where:
        constructedWriteConcern                           | constantWriteConcern
        new WriteConcern((Object) null, null, null) | WriteConcern.ACKNOWLEDGED
        new WriteConcern(1)                               | WriteConcern.W1
        new WriteConcern(2)                               | WriteConcern.W2
        new WriteConcern(3)                               | WriteConcern.W3
        new WriteConcern(0)                               | WriteConcern.UNACKNOWLEDGED
        WriteConcern.ACKNOWLEDGED.withJournal(true)   | WriteConcern.JOURNALED
        new WriteConcern('majority')                      | WriteConcern.MAJORITY
    }

    def 'test isAcknowledged'() {
        expect:
        writeConcern.isAcknowledged() == acknowledged

        where:
        writeConcern                                               | acknowledged
        WriteConcern.ACKNOWLEDGED                                  | true
        WriteConcern.W1                                            | true
        WriteConcern.W2                                            | true
        WriteConcern.W3                                            | true
        WriteConcern.MAJORITY                                      | true
        WriteConcern.UNACKNOWLEDGED                                | false
        WriteConcern.UNACKNOWLEDGED.withWTimeout(10, MILLISECONDS) | false
        WriteConcern.UNACKNOWLEDGED.withJournal(false)      | false
    }

    def 'test value of'() {
        expect:
        wc == valueOf

        where:
        wc                        | valueOf
        WriteConcern.ACKNOWLEDGED | WriteConcern.valueOf('ACKNOWLEDGED')
        WriteConcern.ACKNOWLEDGED | WriteConcern.valueOf('acknowledged')
        null                      | WriteConcern.valueOf('blahblah')
    }

    def 'write concern should know if it is the server default'() {
        expect:
        WriteConcern.ACKNOWLEDGED.serverDefault
        !WriteConcern.UNACKNOWLEDGED.serverDefault
        !WriteConcern.ACKNOWLEDGED.withJournal(false).serverDefault
        !WriteConcern.ACKNOWLEDGED.withWTimeout(0, MILLISECONDS).serverDefault
    }

    def 'should throw when w is -1'() {
        when:
        new WriteConcern(-1)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw when w is null'() {
        when:
        new WriteConcern((String) null)

        then:
        thrown(IllegalArgumentException)
    }
}
