/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package com.mongodb.operation

import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.WriteRequest
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

class InsertRequestSpecification extends Specification {

    def 'should have correct type'() {
        expect:
        new InsertRequest(new BsonDocument()).getType() == WriteRequest.Type.INSERT
    }

    def 'should not allow null document'() {
        when:
        new InsertRequest(null)

        then:
        thrown(IllegalArgumentException)
    }


    def 'should set fields from constructor'() {
        given:
        def document = new BsonDocument('_id', new BsonInt32(1))

        when:
        def insertRequest = new InsertRequest(document)

        then:
        insertRequest.document == document
    }
}
