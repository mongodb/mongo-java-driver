/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.reactivestreams.client

import spock.lang.Specification
import com.mongodb.async.client.MongoClients as WrappedMongoClients


class MongoClientsSpecification extends Specification {


    def 'should have the same methods as the wrapped MongoClients'() {
        given:
        def wrapped = (com.mongodb.async.client.MongoClients.methods*.name).sort()
        def local = MongoClients.methods*.name.sort()
        local.remove(0) // remove 1 extra create method MongoClients.create(async.MongoClient)

        expect:
        wrapped == local
    }

    def 'should return the same default codec registry'() {
        expect:
        MongoClients.getDefaultCodecRegistry() == WrappedMongoClients.getDefaultCodecRegistry()
    }

}
