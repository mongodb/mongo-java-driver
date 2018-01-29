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

import static com.mongodb.ErrorCategory.DUPLICATE_KEY
import static com.mongodb.ErrorCategory.EXECUTION_TIMEOUT
import static com.mongodb.ErrorCategory.UNCATEGORIZED
import static com.mongodb.ErrorCategory.fromErrorCode

class ErrorCategorySpecification extends Specification {

    def 'should categorize duplicate key errors'() {
        expect:
        fromErrorCode(11000) == DUPLICATE_KEY
        fromErrorCode(11001) == DUPLICATE_KEY
        fromErrorCode(12582) == DUPLICATE_KEY
    }

    def 'should categorize execution timeout errors'() {
        expect:
        fromErrorCode(50) == EXECUTION_TIMEOUT
    }

    def 'should categorize uncategorized errors'() {
        expect:
        fromErrorCode(0) == UNCATEGORIZED
    }

}
