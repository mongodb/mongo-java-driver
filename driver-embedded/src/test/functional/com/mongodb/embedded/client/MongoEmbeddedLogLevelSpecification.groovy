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

package com.mongodb.embedded.client

import com.mongodb.embedded.capi.LogLevel
import spock.lang.Specification

class MongoEmbeddedLogLevelSpecification extends Specification {

    def 'should mirror capi LogLevel'() {
        expect:
        mongoEmbeddedLogLevel.toCapiLogLevel() == logLevel

        where:
        mongoEmbeddedLogLevel           | logLevel
        MongoEmbeddedLogLevel.LOGGER    | LogLevel.LOGGER
        MongoEmbeddedLogLevel.NONE      | LogLevel.NONE
        MongoEmbeddedLogLevel.STDERR    | LogLevel.STDERR
        MongoEmbeddedLogLevel.STDOUT    | LogLevel.STDOUT
    }

    def 'should have the same named enum constants as LogLevel'() {
        when:
        // A regression test so that if anymore enums are added then MongoEmbeddedLogLevel should be updated
        def actual = MongoEmbeddedLogLevel.getEnumConstants()*.name().sort()
        def expected = LogLevel.getEnumConstants()*.name().sort()

        then:
        actual == expected
    }

}
