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

class TransactionOptionsSpecification extends Specification {
    def 'should have correct defaults'() {
        when:
        def options = TransactionOptions.builder().build()

        then:
        options.getReadConcern() == null
        options.getWriteConcern() == null
        options.getReadPreference() == null
    }

    def 'should apply options set in builder'() {
        when:
        def options = TransactionOptions.builder()
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.JOURNALED)
                .readPreference(ReadPreference.secondary())
                .build()

        then:
        options.readConcern == ReadConcern.LOCAL
        options.writeConcern == WriteConcern.JOURNALED
        options.readPreference == ReadPreference.secondary()
    }

    def 'should merge'() {
        given:
        def first = TransactionOptions.builder().build();
        def second = TransactionOptions.builder().readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.secondary())
                .build()
        def third = TransactionOptions.builder()
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.W2)
                .readPreference(ReadPreference.nearest())
                .build()

        expect:
        TransactionOptions.merge(first, second) == second
        TransactionOptions.merge(second, first) == second
        TransactionOptions.merge(second, third) == second
        TransactionOptions.merge(third, second) == third
    }
}
