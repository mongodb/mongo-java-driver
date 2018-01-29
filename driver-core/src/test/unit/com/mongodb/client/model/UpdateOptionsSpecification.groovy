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

package com.mongodb.client.model

import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

class UpdateOptionsSpecification extends Specification {
    def 'should have the expected defaults'() {
        when:
        def options = new UpdateOptions()

        then:
        !options.isUpsert()
        options.getBypassDocumentValidation() == null
        options.getCollation() == null
    }

    def 'should set upsert'() {
        expect:
        new UpdateOptions().upsert(upsert).isUpsert() == upsert

        where:
        upsert << [true, false]
    }

    def 'should set bypassDocumentValidation'() {
        expect:
        new UpdateOptions().bypassDocumentValidation(bypassValidation).getBypassDocumentValidation() == bypassValidation

        where:
        bypassValidation << [null, true, false]
    }

    def 'should set collation'() {
        expect:
        new UpdateOptions().collation(collation).getCollation() == collation

        where:
        collation << [null, Collation.builder().locale('en').build()]
    }

    def 'should set array filters'() {
        expect:
        new UpdateOptions().arrayFilters(arrayFilters).getArrayFilters() == arrayFilters

        where:
        arrayFilters << [null, [], [new BsonDocument('a.b', new BsonInt32(1))]]
    }
}
