/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb

import spock.lang.Specification

import java.util.concurrent.TimeUnit

class TransactionOptionsSpecification extends Specification {
    def 'should have correct defaults'() {
        when:
        def options = TransactionOptions.builder().build()

        then:
        options.getReadConcern() == null
        options.getWriteConcern() == null
        options.getReadPreference() == null
        options.getMaxCommitTime(TimeUnit.MILLISECONDS) == null
    }

    def 'should throw an exception if the timeout is invalid'() {
        given:
        def builder = TransactionOptions.builder()


        when:
        builder.timeout(500, TimeUnit.NANOSECONDS)

        then:
        thrown(IllegalArgumentException)

        when:
        builder.timeout(-1, TimeUnit.SECONDS).build()

        then:
        thrown(IllegalArgumentException)
    }

    def 'should apply options set in builder'() {
        when:
        def options = TransactionOptions.builder()
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.JOURNALED)
                .readPreference(ReadPreference.secondary())
                .maxCommitTime(5, TimeUnit.SECONDS)
                .timeout(null, TimeUnit.MILLISECONDS)
                .build()

        then:
        options.readConcern == ReadConcern.LOCAL
        options.writeConcern == WriteConcern.JOURNALED
        options.readPreference == ReadPreference.secondary()
        options.getMaxCommitTime(TimeUnit.MILLISECONDS) == 5000
        options.getMaxCommitTime(TimeUnit.SECONDS) == 5
        options.getTimeout(TimeUnit.MILLISECONDS) == null
    }

    def 'should merge'() {
        given:
        def first = TransactionOptions.builder().build()
        def second = TransactionOptions.builder().readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.secondary())
                .maxCommitTime(5, TimeUnit.SECONDS)
                .timeout(123, TimeUnit.MILLISECONDS)
                .build()
        def third = TransactionOptions.builder()
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.W2)
                .readPreference(ReadPreference.nearest())
                .maxCommitTime(10, TimeUnit.SECONDS)
                .timeout(123, TimeUnit.MILLISECONDS)
                .build()

        expect:
        TransactionOptions.merge(first, second) == second
        TransactionOptions.merge(second, first) == second
        TransactionOptions.merge(second, third) == second
        TransactionOptions.merge(third, second) == third
    }
}
