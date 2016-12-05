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

import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteRequest
import com.mongodb.client.model.Collation
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

class UpdateRequestSpecification extends Specification {

    def 'should have correct type'() {
        expect:
        new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE).getType() == WriteRequest.Type.UPDATE
        new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE).getType() == WriteRequest.Type.REPLACE
    }

    def 'should throw if type is not update or replace'() {
        when:
        new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.INSERT)

        then:
        thrown(IllegalArgumentException)

        when:
        new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.DELETE)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should not allow null filter'() {
        when:
        new UpdateRequest(null, new BsonDocument(), WriteRequest.Type.UPDATE)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should not allow null update'() {
        when:
        new UpdateRequest(new BsonDocument(), null, WriteRequest.Type.UPDATE)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should set fields from constructor'() {
        given:
        def filter = new BsonDocument('_id', new BsonInt32(1))
        def update = new BsonDocument('$set', new BsonDocument('x', BsonBoolean.TRUE))

        when:
        def updateRequest = new UpdateRequest(filter, update, WriteRequest.Type.UPDATE)

        then:
        updateRequest.filter == filter
        updateRequest.update == update
    }

    def 'multi property should default to true'() {
        expect:
        new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE).multi
        !new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE).multi
    }

    def 'should set multi property'() {
        expect:
        !new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE).multi(false).isMulti()
        !new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE).multi(false).isMulti()
    }

    def 'should throw if multi set to true on a replace'() {
        when:
        new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE).multi(true)

        then:
        thrown(IllegalArgumentException)
    }

    def 'upsert property should default to false'() {
        expect:
        !new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE).upsert
    }

    def 'should set upsert property'() {
        expect:
        new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE).upsert(true).isUpsert()
    }

    def 'should set collation property'() {
        when:
        def collation = Collation.builder().locale('en').build()

        then:
        new UpdateRequest(new BsonDocument(), new BsonDocument(), type).collation(null).getCollation() == null
        new UpdateRequest(new BsonDocument(), new BsonDocument(), type).collation(collation).getCollation() == collation

        where:
        type << [WriteRequest.Type.UPDATE, WriteRequest.Type.REPLACE]
    }
}
