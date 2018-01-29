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

package com.mongodb.client.model.changestream

import spock.lang.Specification

class OperationTypeSpecification extends Specification {

    def 'should return the expected string value'() {
        expect:
        operationType.getValue() == expectedString

        where:
        operationType            | expectedString
        OperationType.DELETE     | 'delete'
        OperationType.INSERT     | 'insert'
        OperationType.INVALIDATE | 'invalidate'
        OperationType.REPLACE    | 'replace'
        OperationType.UPDATE     | 'update'
    }

    def 'should support valid string representations'() {
        expect:
        OperationType.fromString(stringValue) == operationType

        where:
        operationType            | stringValue
        OperationType.DELETE     | 'delete'
        OperationType.INSERT     | 'insert'
        OperationType.INVALIDATE | 'invalidate'
        OperationType.REPLACE    | 'replace'
        OperationType.UPDATE     | 'update'
    }

    def 'should throw an illegal argument exception for invalid values'() {
        when:
        OperationType.fromString(stringValue)

        then:
        thrown(IllegalArgumentException)

        where:
        stringValue << [null, 'info']
    }
}
