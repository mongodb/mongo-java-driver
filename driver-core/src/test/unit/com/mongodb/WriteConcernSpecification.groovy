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
import static java.util.concurrent.TimeUnit.SECONDS

@SuppressWarnings('deprecation')
class WriteConcernSpecification extends Specification {

    @Unroll
    def 'constructors should set up write concern #wc correctly'() {
        expect:
        wc.getWObject() == w;
        wc.getWTimeout(MILLISECONDS) == wTimeout;
        wc.getFsync() == fsync;
        wc.getJournal() == journal;

        where:
        wc                                                | w          | wTimeout | fsync | journal
        new WriteConcern()                                | 0          | null     | false | null
        new WriteConcern(1)                               | 1          | null     | false | null
        new WriteConcern(1, 10)                           | 1          | 10       | false | null
        new WriteConcern(true)                            | null       | null     | true  | null
        new WriteConcern(1, 10, true)                     | 1          | 10       | true  | null
        new WriteConcern(1, 10, false, true)              | 1          | 10       | false | true
        new WriteConcern(1, 10, false, true)              | 1          | 10       | false | true
        new WriteConcern((Object) null, 0, false, false)  | null       | 0        | false | false
        new WriteConcern('majority')                      | 'majority' | null     | false | null
        new WriteConcern('dc1', 10, false, true)          | 'dc1'      | 10       | false | true
        new WriteConcern('dc1', 10, false, true)          | 'dc1'      | 10       | false | true
    }

    def 'test journal getters'() {
        expect:
        wc.getJournal() == journal
        wc.getJ() == j

        where:
        wc                                        | journal | j
        new WriteConcern(null, null, null, null)  | null    | false
        new WriteConcern(null, null, null, false) | false   | false
        new WriteConcern(null, null, null, true)  | true    | true
    }

    def 'test fsync getters'() {
        expect:
        wc.getFsync() == fsync
        wc.fsync() == fsync

        where:
        wc                                        | fsync
        new WriteConcern(null, null, null, null)  | false
        new WriteConcern(null, null, false, null) | false
        new WriteConcern(null, null, true, null)  | true
    }

    def 'test wTimeout getters'() {
        expect:
        wc.getWTimeout(MILLISECONDS) == wTimeout
        wc.getWTimeout(MICROSECONDS) == (wTimeout == null ? null : wTimeout * 1000)
        wc.getWtimeout() == (wTimeout == null ? 0 : wTimeout)

        where:
        wc                                        | wTimeout
        new WriteConcern(null, null, null, null)  | null
        new WriteConcern(null, 1000 , null, null) | 1000
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
        WriteConcern.UNACKNOWLEDGED.withW(1) == new WriteConcern(1, null, null, null);
        WriteConcern.UNACKNOWLEDGED.withW('dc1') == new WriteConcern('dc1', null, null, null);

        when:
        WriteConcern.UNACKNOWLEDGED.withW(null)

        then:
        thrown(IllegalArgumentException)

        when:
        WriteConcern.UNACKNOWLEDGED.withW(-1)

        then:
        thrown(IllegalArgumentException)

        when:
        WriteConcern.UNACKNOWLEDGED.withFsync(true)

        then:
        thrown(IllegalArgumentException)

        when:
        WriteConcern.UNACKNOWLEDGED.withJournal(true)

        then:
        thrown(IllegalArgumentException)
    }

    def 'test withFsync methods'() {
        expect:
        new WriteConcern(null, null, null, null).withFsync(true) == new WriteConcern(null, null, true, null);
        new WriteConcern(2, 100, false, false).withFsync(true) == new WriteConcern(2, 100, true, false);
        new WriteConcern(2, 100, true, true).withFsync(false) == new WriteConcern(2, 100, false, true);
    }

    def 'test withJournal methods'() {
        expect:
        new WriteConcern(null, null, null, null).withJournal(true) == new WriteConcern(null, null, null, true);
        new WriteConcern(2, 100, false, false).withJournal(true) == new WriteConcern(2, 100, false, true);
        new WriteConcern(2, 100, true, true).withJournal(false) == new WriteConcern(2, 100, true, false);
        new WriteConcern(2, 100, true, true).withJournal(null) == new WriteConcern(2, 100, true, null);

        new WriteConcern(null, null, null, null).withJ(true) == new WriteConcern(null, null, null, true);
        new WriteConcern(2, 100, false, false).withJ(true) == new WriteConcern(2, 100, false, true);
        new WriteConcern(2, 100, true, true).withJ(false) == new WriteConcern(2, 100, true, false);
    }

    def 'test withWTimeout methods'() {
        expect:
        new WriteConcern(null, null, null, null).withWTimeout(0, MILLISECONDS) == new WriteConcern(null, 0, null, null)
        new WriteConcern(null, null, null, null).withWTimeout(1000, MILLISECONDS) == new WriteConcern(null, 1000, null, null)
        new WriteConcern(2, null, true, false).withWTimeout(1000, MILLISECONDS) == new WriteConcern(2, 1000, true, false);
        new WriteConcern(2, null, true, false).withWTimeout(Integer.MAX_VALUE, MILLISECONDS) == new WriteConcern(2, Integer.MAX_VALUE,
                                                                                                                 true, false);
        new WriteConcern(2, null, true, false).withWTimeout(100, SECONDS) == new WriteConcern(2, 100000, true, false);

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
        wc.asDocument() == commandDocument;

        where:
        wc                                | commandDocument
        WriteConcern.UNACKNOWLEDGED       | new BsonDocument('w', new BsonInt32(0))
        WriteConcern.ACKNOWLEDGED         | new BsonDocument()
        WriteConcern.W2 | new BsonDocument('w', new BsonInt32(2))
        WriteConcern.JOURNALED            | new BsonDocument('j', BsonBoolean.TRUE)
        WriteConcern.FSYNCED              | new BsonDocument('fsync', BsonBoolean.TRUE)
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
        new WriteConcern(1, 0, false, false) | new WriteConcern(1, 0, false, true) | false
        new WriteConcern(1, 0, false, false) | new WriteConcern(1, 0, true, false) | false
        new WriteConcern(1, 0)               | new WriteConcern(1, 1)              | false
    }

    def 'test hashCode'() {
        expect:
        wc.hashCode() == hashCode

        where:
        wc                                   | hashCode
        WriteConcern.ACKNOWLEDGED            | 0
        WriteConcern.W1                      | 29791
        WriteConcern.W2                      | 59582
        WriteConcern.MAJORITY                | -1401337973
        new WriteConcern(1, 0, false, false) | 69375
        new WriteConcern(1, 0, true, true)   | 69183
    }

    def 'test constants'() {
        expect:
        constructedWriteConcern == constantWriteConcern

        where:
        constructedWriteConcern                           | constantWriteConcern
        new WriteConcern((Object) null, null, null, null) | WriteConcern.ACKNOWLEDGED
        new WriteConcern(1)                               | WriteConcern.W1
        new WriteConcern(2)                               | WriteConcern.W2
        new WriteConcern(3)                               | WriteConcern.W3
        new WriteConcern(0)                               | WriteConcern.UNACKNOWLEDGED
        WriteConcern.ACKNOWLEDGED.withFsync(true)         | WriteConcern.FSYNCED
        WriteConcern.ACKNOWLEDGED.withFsync(true)         | WriteConcern.FSYNC_SAFE
        WriteConcern.ACKNOWLEDGED.withJ(true)             | WriteConcern.JOURNALED
        new WriteConcern(2)                               | WriteConcern.REPLICA_ACKNOWLEDGED
        new WriteConcern(2)                               | WriteConcern.REPLICAS_SAFE
        new WriteConcern('majority')                      | WriteConcern.MAJORITY
    }

    def 'test isAcknowledged'() {
        expect:
        writeConcern.isAcknowledged() == acknowledged
        writeConcern.callGetLastError() == acknowledged

        where:
        writeConcern                                               | acknowledged
        WriteConcern.ACKNOWLEDGED                                  | true
        WriteConcern.W1                                            | true
        WriteConcern.W2                                            | true
        WriteConcern.W3                                            | true
        WriteConcern.MAJORITY                                      | true
        WriteConcern.UNACKNOWLEDGED                                | false
        WriteConcern.UNACKNOWLEDGED.withWTimeout(10, MILLISECONDS) | false
        WriteConcern.UNACKNOWLEDGED.withFsync(false)         | false
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
        !WriteConcern.ACKNOWLEDGED.withFsync(false).serverDefault
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

    def 'majority write concern'() {
        given:
        def majority = new WriteConcern.Majority()

        expect:
        majority.getWString() == 'majority'
        majority.getWtimeout() == 0
        !majority.getFsync()
        !majority.getJ()
    }

    def 'majority with options write concern'() {
        given:
        def majorityWithOptions = new WriteConcern.Majority(10, true, true)

        expect:
        majorityWithOptions.getWString() == 'majority'
        majorityWithOptions.getWtimeout() == 10
        majorityWithOptions.getFsync()
        majorityWithOptions.getJ()
    }
}
