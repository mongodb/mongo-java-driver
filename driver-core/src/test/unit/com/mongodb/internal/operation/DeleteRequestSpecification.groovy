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



package com.mongodb.internal.operation

import com.mongodb.client.model.Collation
import com.mongodb.internal.bulk.DeleteRequest
import com.mongodb.internal.bulk.WriteRequest
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

class DeleteRequestSpecification extends Specification {

    def 'should have correct type'() {
        expect:
        new DeleteRequest(new BsonDocument()).getType() == WriteRequest.Type.DELETE
    }

    def 'should not allow null filter'() {
        when:
        new DeleteRequest(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should set fields from constructor'() {
        given:
        def filter = new BsonDocument('_id', new BsonInt32(1))

        when:
        def removeRequest = new DeleteRequest(filter)

        then:
        removeRequest.filter == filter
    }

    def 'multi property should default to true'() {
        expect:
        new DeleteRequest(new BsonDocument()).multi
    }

    def 'should set multi property'() {
        expect:
        !new DeleteRequest(new BsonDocument()).multi(false).isMulti()
    }

    def 'should set collation property'() {
        when:
        def collation = Collation.builder().locale('en').build()

        then:
        new DeleteRequest(new BsonDocument()).collation(null).getCollation() == null
        new DeleteRequest(new BsonDocument()).collation(collation).getCollation() == collation
    }
}
