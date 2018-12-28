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

package com.mongodb.client

import com.mongodb.ClusterFixture
import com.mongodb.MongoException

import javax.naming.Reference
import javax.naming.StringRefAddr

class MongoClientFactorySpecification extends FunctionalSpecification {
    def mongoClientFactory = new MongoClientFactory()

    def 'should create MongoClient from environment'() {
        given:
        def environment = new Hashtable<String, String>()
        environment.put('connectionString', ClusterFixture.getConnectionString().getConnectionString())

        when:
        MongoClient client = mongoClientFactory.getObjectInstance(null, null, null, environment) as MongoClient

        then:
        client != null

        cleanup:
        client?.close()
    }

    def 'should create MongoClient from obj that is of type Reference'() {
        given:
        def environment = new Hashtable<String, String>()
        def reference = new Reference(null, new StringRefAddr('connectionString',
                ClusterFixture.getConnectionString().getConnectionString()))

        when:
        MongoClient client = mongoClientFactory.getObjectInstance(reference, null, null, environment) as MongoClient

        then:
        client != null

        cleanup:
        client?.close()
    }

    def 'should throw if no connection string is provided'() {
        given:
        def environment = new Hashtable<String, String>()

        when:
        mongoClientFactory.getObjectInstance(null, null, null, environment)

        then:
        thrown(MongoException)
    }
}

