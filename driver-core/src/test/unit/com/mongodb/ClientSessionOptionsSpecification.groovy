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

import spock.lang.Specification

import java.util.concurrent.TimeUnit

class ClientSessionOptionsSpecification extends Specification {

    def 'should have correct defaults'() {
        when:
        def options = ClientSessionOptions.builder().build()

        then:
        options.isCausallyConsistent() == null
        options.defaultTransactionOptions == TransactionOptions.builder().build()
    }

    def 'should apply options set in builder'() {
        when:
        def options = ClientSessionOptions.builder()
                .causallyConsistent(causallyConsistent)
                .defaultTransactionOptions(transactionOptions)
                .build()

        then:
        options.isCausallyConsistent() == causallyConsistent
        options.defaultTransactionOptions == transactionOptions

        where:
        causallyConsistent << [true, false]
        transactionOptions << [TransactionOptions.builder().build(), TransactionOptions.builder().readConcern(ReadConcern.LOCAL).build()]
    }

    def 'should throw an exception if the defaultTimeout is set and negative'() {
        given:
        def builder = ClientSessionOptions.builder()

        when:
        builder.defaultTimeout(500, TimeUnit.NANOSECONDS)

        then:
        thrown(IllegalArgumentException)

        when:
        builder.defaultTimeout(-1, TimeUnit.SECONDS)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should apply options to builder'() {
        expect:
        ClientSessionOptions.builder(baseOptions).build() == baseOptions

        where:
        baseOptions << [ClientSessionOptions.builder().build(),
                        ClientSessionOptions.builder()
                                .causallyConsistent(true)
                                .defaultTransactionOptions(TransactionOptions.builder()
                                .writeConcern(WriteConcern.MAJORITY)
                                .readConcern(ReadConcern
                                .MAJORITY).build())
                                .build()]
    }
}
