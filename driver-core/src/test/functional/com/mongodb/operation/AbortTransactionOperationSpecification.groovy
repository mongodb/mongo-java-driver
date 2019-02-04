/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.mongodb.operation

import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonDocument

import java.util.concurrent.TimeUnit

import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.MAJORITY


class AbortTransactionOperationSpecification extends OperationFunctionalSpecification {

    def 'should produce the expected command'() {
        given:
        def cannedResult = BsonDocument.parse('{value: {}}')
        def expectedCommand = BsonDocument.parse('{abortTransaction: 1}')

        when:
        def operation = new AbortTransactionOperation(ACKNOWLEDGED)

        then:
        testOperationInTransaction(operation, [4, 0, 0], expectedCommand, async, cannedResult)

        when:
        operation = new AbortTransactionOperation(MAJORITY)
        expectedCommand.put('writeConcern', MAJORITY.asDocument())

        then:
        testOperationInTransaction(operation, [4, 0, 0], expectedCommand, async, cannedResult, true)

        where:
        async << [true, false]
    }

    def 'should retry if the connection initially fails'() {
        given:
        def cannedResult = BsonDocument.parse('{value: {}}')
        def expectedCommand = BsonDocument.parse('{abortTransaction: 1, writeConcern: {w: "majority", wtimeout: 10}}')

        when:
        def writeConcern = MAJORITY.withWTimeout(10, TimeUnit.MILLISECONDS)
        def operation = new AbortTransactionOperation(writeConcern)

        then:
        testOperationRetries(operation, [4, 0, 0], expectedCommand, async, cannedResult, true)

        when:
        writeConcern = MAJORITY
        operation = new AbortTransactionOperation(writeConcern)
        expectedCommand.put('writeConcern', writeConcern.asDocument())

        then:
        testOperationRetries(operation, [4, 0, 0], expectedCommand, async, cannedResult, true)

        when:
        writeConcern = ACKNOWLEDGED
        operation = new AbortTransactionOperation(writeConcern)
        expectedCommand.remove('writeConcern')

        then:
        testOperationRetries(operation, [4, 0, 0], expectedCommand, async, cannedResult, true)

        where:
        async << [true, false]
    }
}
