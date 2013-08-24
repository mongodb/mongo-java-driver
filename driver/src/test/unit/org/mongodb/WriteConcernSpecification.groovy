/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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



package org.mongodb

import spock.lang.Specification
import spock.lang.Unroll

class WriteConcernSpecification extends Specification {

    @Unroll
    def 'constructors should set up write concern #wc correctly'() {
        expect:
        wc.getW() == w;
        wc.getWtimeout() == wTimeout;
        wc.getFsync() == fsync;
        wc.getJ() == j;
        wc.getContinueOnErrorForInsert() == continueOnErrorForInsert;

        where:
        wc                                         | w | wTimeout | fsync | j     | continueOnErrorForInsert
        new WriteConcern()                         | 0 | 0        | false | false | false
        new WriteConcern(1)                        | 1 | 0        | false | false | false
        new WriteConcern(1, 10)                    | 1 | 10       | false | false | false
        new WriteConcern(true)                     | 1 | 0        | true  | false | false
        new WriteConcern(1, 10, true)              | 1 | 10       | true  | false | false
        new WriteConcern(1, 10, false, true)       | 1 | 10       | false | true  | false
        new WriteConcern(1, 10, false, true, true) | 1 | 10       | false | true  | true
    }

    @Unroll
    def 'constructors should set up write concern #wc correctly with wString'() {
        expect:
        wc.getWString() == wString;
        wc.getWtimeout() == wTimeout;
        wc.getFsync() == fsync;
        wc.getJ() == j;
        wc.getContinueOnErrorForInsert() == continueOnErrorForInsert;

        where:
        wc                                             | wString    | wTimeout | fsync | j     | continueOnErrorForInsert
        new WriteConcern('majority')                   | 'majority' | 0        | false | false | false
        new WriteConcern('dc1', 10, false, true)       | 'dc1'      | 10       | false | true  | false
        new WriteConcern('dc1', 10, false, true, true) | 'dc1'      | 10       | false | true  | true
    }

    def 'test getters'() {
        expect:
        wc.isAcknowledged() == getLastError;
        wc.raiseNetworkErrors() == raiseNetworkErrors;
        wc.getWObject() == wObject;

        where:
        wc                                            | getLastError | raiseNetworkErrors | wObject
        new WriteConcern('dc1', 10, true, true, true) | true         | true               | 'dc1'
        new WriteConcern(-1, 10, false, true, true)   | false        | false              | -1
    }

    def 'test with methods'() {
        expect:
        WriteConcern.ACKNOWLEDGED == WriteConcern.UNACKNOWLEDGED.withW(1);
        WriteConcern.FSYNCED == WriteConcern.ACKNOWLEDGED.withFsync(true);
        WriteConcern.JOURNALED == WriteConcern.ACKNOWLEDGED.withJ(true);
        new WriteConcern('dc1') == WriteConcern.UNACKNOWLEDGED.withW('dc1');
        new WriteConcern('dc1', 0, true, false) == new WriteConcern('dc1').withFsync(true);
        new WriteConcern('dc1', 0, false, true) == new WriteConcern('dc1').withJ(true);
        new WriteConcern('dc1', 0, false, false, true) == new WriteConcern('dc1').withContinueOnErrorForInsert(true);
        new WriteConcern(1, 0, false, false, true) == WriteConcern.ACKNOWLEDGED.withContinueOnErrorForInsert(true);
    }

    def 'test command'() {
        expect:
        wc.asDocument() == commandDocument;

        where:
        wc                                | commandDocument
        WriteConcern.UNACKNOWLEDGED       | new Document('getlasterror', 1)
        WriteConcern.ACKNOWLEDGED         | new Document('getlasterror', 1)
        WriteConcern.REPLICA_ACKNOWLEDGED | new Document('getlasterror', 1).append('w', 2)
        WriteConcern.FSYNCED              | new Document('getlasterror', 1).append('fsync', true)
        WriteConcern.JOURNALED            | new Document('getlasterror', 1).append('j', true)
        new WriteConcern('majority')      | new Document('getlasterror', 1).append('w', 'majority')
        new WriteConcern(1, 100)          | new Document('getlasterror', 1).append('wtimeout', 100)
    }

    @SuppressWarnings('ExplicitCallToEqualsMethod')
    def 'test equals'() {
        expect:
        wc.equals(compareTo) == expectedResult

        where:
        wc                                         | compareTo                                   | expectedResult
        WriteConcern.ACKNOWLEDGED                  | WriteConcern.ACKNOWLEDGED                   | true
        WriteConcern.ACKNOWLEDGED                  | null                                        | false
        WriteConcern.ACKNOWLEDGED                  | WriteConcern.UNACKNOWLEDGED                 | false
        new WriteConcern(1, 0, false, false, true) | new WriteConcern(1, 0, false, false, false) | false
        new WriteConcern(1, 0, false, false)       | new WriteConcern(1, 0, false, true)         | false
        new WriteConcern(1, 0, false, false)       | new WriteConcern(1, 0, true, false)         | false
        new WriteConcern(1, 0)                     | new WriteConcern(1, 1)                      | false
    }

    def 'test constants'() {
        expect:
        constructedWriteConcern == constantWriteConcern

        where:
        constructedWriteConcern             | constantWriteConcern
        new WriteConcern(-1)                | WriteConcern.ERRORS_IGNORED
        new WriteConcern(1)                 | WriteConcern.ACKNOWLEDGED
        new WriteConcern(0)                 | WriteConcern.UNACKNOWLEDGED
        new WriteConcern(1, 0, true)        | WriteConcern.FSYNCED
        new WriteConcern(1, 0, false, true) | WriteConcern.JOURNALED
        new WriteConcern(2)                 | WriteConcern.REPLICA_ACKNOWLEDGED
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
}
