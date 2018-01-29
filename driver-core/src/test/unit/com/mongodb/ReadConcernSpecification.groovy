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

import org.bson.BsonDocument
import spock.lang.Specification

class ReadConcernSpecification extends Specification {

    def 'should have the expected read concern levels'() {
        expect:
        staticValue == expectedReadConcern
        staticValue.getLevel() == expectedLevel

        where:
        staticValue              | expectedLevel                 | expectedReadConcern
        ReadConcern.DEFAULT      | null                          | new ReadConcern()
        ReadConcern.LOCAL        | ReadConcernLevel.LOCAL        | new ReadConcern(ReadConcernLevel.LOCAL)
        ReadConcern.MAJORITY     | ReadConcernLevel.MAJORITY     | new ReadConcern(ReadConcernLevel.MAJORITY)
        ReadConcern.LINEARIZABLE | ReadConcernLevel.LINEARIZABLE | new ReadConcern(ReadConcernLevel.LINEARIZABLE)
    }

    def 'should create the expected Documents'() {
        expect:
        staticValue.asDocument() == expected

        where:
        staticValue              | expected
        ReadConcern.DEFAULT      | BsonDocument.parse('{}')
        ReadConcern.LOCAL        | BsonDocument.parse('{level: "local"}')
        ReadConcern.MAJORITY     | BsonDocument.parse('{level: "majority"}')
        ReadConcern.LINEARIZABLE | BsonDocument.parse('{level: "linearizable"}')
    }

    def 'should have the correct value for isServerDefault'() {
        expect:
        staticValue.isServerDefault() == expected

        where:
        staticValue              | expected
        ReadConcern.DEFAULT      | true
        ReadConcern.LOCAL        | false
        ReadConcern.MAJORITY     | false
        ReadConcern.LINEARIZABLE | false
    }
}
