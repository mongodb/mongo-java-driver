/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

@SuppressWarnings('deprecation')
class WriteConcernSpecification extends Specification {

    @Unroll
    def 'constructors should set up write concern #wc correctly'() {
        expect:
        wc.getWObject() == w;
        wc.getWtimeout() == wTimeout;
        wc.getFsync() == fsync;
        wc.getJ() == j;

        where:
        wc                                                | w    | wTimeout | fsync | j
        new WriteConcern()                                | 0    | 0        | false | false
        new WriteConcern(1)                               | 1    | 0        | false | false
        new WriteConcern(1, 10)                           | 1    | 10       | false | false
        new WriteConcern(true)                            | 1    | 0        | true  | false
        new WriteConcern(1, 10, true)                     | 1    | 10       | true  | false
        new WriteConcern(1, 10, false, true)              | 1    | 10       | false | true
        new WriteConcern(1, 10, false, true)              | 1    | 10       | false | true
        new WriteConcern((Object) null, 0, false, false)  | null | 0        | false | false
    }

    @Unroll
    def 'constructors should set up write concern #wc correctly with wString'() {
        expect:
        wc.getWString() == wString;
        wc.getWtimeout() == wTimeout;
        wc.getFsync() == fsync;
        wc.getJ() == j;

        where:
        wc                                       | wString    | wTimeout | fsync | j
        new WriteConcern('majority')             | 'majority' | 0        | false | false
        new WriteConcern('dc1', 10, false, true) | 'dc1'      | 10       | false | true
        new WriteConcern('dc1', 10, false, true) | 'dc1'      | 10       | false | true
    }

    def 'test getters'() {
        expect:
        wc.isAcknowledged() == getLastError;
        wc.getWObject() == wObject;

        where:
        wc                                      | getLastError | wObject
        new WriteConcern('dc1', 10, true, true) | true         | 'dc1'
        new WriteConcern(0, 10, false, true)    | false        | 0
    }

    def 'test with methods'() {
        expect:
        WriteConcern.W1 == WriteConcern.UNACKNOWLEDGED.withW(1);
        WriteConcern.FSYNCED == WriteConcern.ACKNOWLEDGED.withFsync(true);
        WriteConcern.JOURNALED == WriteConcern.ACKNOWLEDGED.withJ(true);
        new WriteConcern(1, 1000) == WriteConcern.ACKNOWLEDGED.withWTimeout(1000)
        new WriteConcern('dc1') == WriteConcern.UNACKNOWLEDGED.withW('dc1');
        new WriteConcern('dc1', 0, true, false) == new WriteConcern('dc1').withFsync(true);
        new WriteConcern('dc1', 0, false, true) == new WriteConcern('dc1').withJ(true);
        new WriteConcern('dc1', 1000, false, false) == new WriteConcern('dc1').withWTimeout(1000);
        new WriteConcern(2, 0, true, false) == new WriteConcern(2).withFsync(true);
        new WriteConcern(2, 0, false, true) == new WriteConcern(2).withJ(true);
        new WriteConcern(2, 1000, false, false) == new WriteConcern(2).withWTimeout(1000);
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
        WriteConcern.JOURNALED            | new BsonDocument('w', new BsonInt32(1)).append('j', BsonBoolean.TRUE)
        WriteConcern.FSYNCED              | new BsonDocument('w', new BsonInt32(1)).append('fsync', BsonBoolean.TRUE)
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
        new WriteConcern(1, 0, false, false) | 29791
        new WriteConcern(1, 0, true, true)   | 29823
    }

    def 'test constants'() {
        expect:
        constructedWriteConcern == constantWriteConcern

        where:
        constructedWriteConcern                          | constantWriteConcern
        new WriteConcern((Object) null, 0, false, false) | WriteConcern.ACKNOWLEDGED
        new WriteConcern(1)                              | WriteConcern.W1
        new WriteConcern(2)                              | WriteConcern.W2
        new WriteConcern(3)                              | WriteConcern.W3
        new WriteConcern(0)                              | WriteConcern.UNACKNOWLEDGED
        new WriteConcern(1, 0, true)                     | WriteConcern.FSYNCED
        new WriteConcern(1, 0, true)                     | WriteConcern.FSYNC_SAFE
        new WriteConcern(1, 0, false, true)              | WriteConcern.JOURNALED
        new WriteConcern(2)                              | WriteConcern.REPLICA_ACKNOWLEDGED
        new WriteConcern(2)                              | WriteConcern.REPLICAS_SAFE
        new WriteConcern('majority')                     | WriteConcern.MAJORITY
    }

    def 'test isAcknowledged'() {
        expect:
        writeConcern.isAcknowledged() == acknowledged
        writeConcern.callGetLastError() == acknowledged

        where:
        writeConcern                        | acknowledged
        WriteConcern.ACKNOWLEDGED           | true
        WriteConcern.W1                     | true
        WriteConcern.W2                     | true
        WriteConcern.W3                     | true
        WriteConcern.MAJORITY               | true
        WriteConcern.UNACKNOWLEDGED         | false
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
        !new WriteConcern(2, 1000).serverDefault
        !new WriteConcern(1, 0, true, false).serverDefault
        !new WriteConcern(1, 0, false, true).serverDefault
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

    def 'should throw ClassCastException if w is not an integer and getW is called'() {
        when:
        new WriteConcern('MyTag').getW()

        then:
        thrown(ClassCastException)
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
