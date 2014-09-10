/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.operation

import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification


class UpdateRequestSpecification extends Specification {

    def 'should have correct type'() {
        expect:
        new UpdateRequest(new BsonDocument(), new BsonDocument()).getType() == WriteRequest.Type.UPDATE

    }

    def 'should not allow null criteria'() {
        when:
        new UpdateRequest(null, new BsonDocument())

        then:
        thrown(IllegalArgumentException)
    }

    def 'should not allow null update'() {
        when:
        new UpdateRequest(new BsonDocument(), null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should set fields from constructor'() {
        given:
        def criteria = new BsonDocument('_id', new BsonInt32(1))
        def update = new BsonDocument('$set', new BsonDocument('x', BsonBoolean.TRUE))

        when:
        def updateRequest = new UpdateRequest(criteria, update)

        then:
        updateRequest.criteria == criteria
        updateRequest.updateOperations == update

    }

    def 'multi property should default to true'() {
        expect:
        new UpdateRequest(new BsonDocument(), new BsonDocument()).multi
    }

    def 'should set multi property'() {
        expect:
        !new UpdateRequest(new BsonDocument(), new BsonDocument()).multi(false).isMulti()
    }

    def 'upsert property should default to false'() {
        expect:
        !new UpdateRequest(new BsonDocument(), new BsonDocument()).upsert
    }

    def 'should set upsert property'() {
        expect:
        new UpdateRequest(new BsonDocument(), new BsonDocument()).upsert(true).isUpsert()
    }

}